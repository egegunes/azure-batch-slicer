import configparser
import datetime
import io
import os
import time

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels


CONTAINER_NAME = "slicefiles"
TASK_NAME = "slicer.py"
TASK_PATH = os.path.join("bin", "slicer.py")
STANDARD_OUT_FILE_NAME = 'stdout.txt'
STANDARD_ERROR_FILE_NAME = 'stderr.txt'


def generate_unique_resource_name(resource_prefix):
    return resource_prefix + "-" + datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")


def select_latest_vm_image_with_node_agent_sku(batch_client, publisher, offer, sku_starts_with):
    # get verified vm image list and node agent sku ids from service
    node_agent_skus = batch_client.account.list_node_agent_skus()

    # pick the latest supported sku
    skus_to_use = [
        (sku, image_ref) for sku in node_agent_skus for image_ref in sorted(
            sku.verified_image_references, key=lambda item: item.sku)
        if image_ref.publisher.lower() == publisher.lower() and
        image_ref.offer.lower() == offer.lower() and
        image_ref.sku.startswith(sku_starts_with)
    ]
    # skus are listed in reverse order, pick first for latest
    sku_to_use, image_ref_to_use = skus_to_use[0]
    return (sku_to_use.id, image_ref_to_use)


def create_sas_token(block_blob_client, container_name, blob_name, permission, expiry=None, timeout=None):
    if expiry is None:
        if timeout is None:
            timeout = 30
        expiry = datetime.datetime.utcnow() + datetime.timedelta(minutes=timeout)
    return block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=permission,
        expiry=expiry
    )


def upload_blob_and_create_sas(block_blob_client, container_name, blob_name, file_name, expiry, timeout=None):
    block_blob_client.create_container(
        container_name,
        fail_on_exist=False
    )

    block_blob_client.create_blob_from_path(
        container_name,
        blob_name,
        file_name
    )

    sas_token = create_sas_token(
        block_blob_client,
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=expiry,
        timeout=timeout
    )

    sas_url = block_blob_client.make_blob_url(
        container_name,
        blob_name,
        sas_token=sas_token
    )

    return sas_url


def create_pool_if_not_exist(batch_client, pool):
    try:
        print("Attempting to create pool:", pool.id)
        batch_client.pool.add(pool)
        print("Created pool:", pool.id)
    except batchmodels.BatchErrorException as e:
        if e.error.code != "PoolExists":
            raise
        else:
            print("Pool {!r} already exists".format(pool.id))


def create_pool(batch_client, block_blob_client, pool_id, vm_size, vm_count):
    block_blob_client.create_container(CONTAINER_NAME, fail_on_exist=False)

    sku_to_use, image_ref_to_use = select_latest_vm_image_with_node_agent_sku(
        batch_client,
        'Canonical',
        'UbuntuServer',
        '18.04'
    )

    sas_url = upload_blob_and_create_sas(
        block_blob_client,
        CONTAINER_NAME,
        TASK_NAME,
        TASK_PATH,
        datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    )

    pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            node_agent_sku_id=sku_to_use
        ),
        vm_size=vm_size,
        target_dedicated_nodes=vm_count,
        start_task=batchmodels.StartTask(
            command_line="python3 " + TASK_NAME,
            resource_files=[
                batchmodels.ResourceFile(
                    file_path=TASK_NAME,
                    blob_source=sas_url
                )
            ]
        )
    )

    create_pool_if_not_exist(batch_client, pool)


def submit_job_and_add_task(batch_client, block_blob_client, job_id, pool_id):
    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id)
    )

    batch_client.job.add(job)

    block_blob_client.create_container(
        CONTAINER_NAME,
        fail_on_exist=False
    )

    sas_url = upload_blob_and_create_sas(
        block_blob_client,
        CONTAINER_NAME,
        TASK_NAME,
        TASK_PATH,
        datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    )

    task = batchmodels.TaskAddParameter(
        id="SliceTask",
        command_line="python3 " + TASK_NAME,
        resource_files=[
            batchmodels.ResourceFile(
                file_path=TASK_NAME,
                blob_source=sas_url
            )
        ]
    )

    batch_client.task.add(job_id=job.id, task=task)


def wait_for_tasks_to_complete(batch_client, job_id, timeout):
    time_to_timeout_at = datetime.datetime.now() + timeout

    while datetime.datetime.now() < time_to_timeout_at:
        print("Checking if all tasks are complete...")
        tasks = batch_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            return
        time.sleep(5)

    raise TimeoutError("Timed out waiting for tasks to complete")


def read_stream_as_string(stream, encoding):
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = 'utf-8'
        return output.getvalue().decode(encoding)
    finally:
        output.close()
    raise RuntimeError('could not write data to stream or decode bytes')


def read_task_file_as_string(batch_client, job_id, task_id, file_name, encoding=None):
    stream = batch_client.file.get_from_task(job_id, task_id, file_name)
    return read_stream_as_string(stream, encoding)


def print_task_output(batch_client, job_id, task_ids, encoding=None):
    for task_id in task_ids:
        file_text = read_task_file_as_string(batch_client, job_id, task_id, STANDARD_OUT_FILE_NAME, encoding)
        print("{} content for task {}: ".format(STANDARD_OUT_FILE_NAME, task_id))
        print(file_text)

        file_text = read_task_file_as_string(batch_client, job_id, task_id, STANDARD_ERROR_FILE_NAME, encoding)
        print("{} content for task {}: ".format(STANDARD_ERROR_FILE_NAME, task_id))
        print(file_text)


def run(config):
    batch_account_key = config.get('Batch', 'batchaccountkey')
    batch_account_name = config.get('Batch', 'batchaccountname')
    batch_service_url = config.get('Batch', 'batchserviceurl')

    storage_account_key = config.get('Storage', 'storageaccountkey')
    storage_account_name = config.get('Storage', 'storageaccountname')
    storage_account_suffix = config.get('Storage', 'storageaccountsuffix')

    delete_container = config.getboolean('Slicer', 'deletecontainer')
    delete_job = config.getboolean('Slicer', 'deletejob')
    delete_pool = config.getboolean('Slicer', 'deletepool')
    pool_vm_size = config.get('Slicer', 'poolvmsize')
    pool_vm_count = config.getint('Slicer', 'poolvmcount')

    credentials = batchauth.SharedKeyCredentials(batch_account_name, batch_account_key)
    batch_client = batch.BatchServiceClient(credentials, base_url=batch_service_url)

    block_blob_client = azureblob.BlockBlobService(
        account_name=storage_account_name,
        account_key=storage_account_key,
        endpoint_suffix=storage_account_suffix
    )

    pool_id = "SlicerPool"
    job_id = generate_unique_resource_name("SliceJob")

    try:
        create_pool(batch_client, block_blob_client, pool_id, pool_vm_size, pool_vm_count)

        submit_job_and_add_task(batch_client, block_blob_client, job_id, pool_id)

        wait_for_tasks_to_complete(batch_client, job_id, datetime.timedelta(minutes=25))

        tasks = batch_client.task.list(job_id)
        task_ids = [task.id for task in tasks]

        print_task_output(batch_client, job_id, task_ids)
    finally:
        if delete_container:
            block_blob_client.delete_container(CONTAINER_NAME, fail_not_exist=False)
        if delete_job:
            print("Deleting job: ", job_id)
            batch_client.job.delete(job_id)
        if delete_pool:
            print("Deleting pool: ", pool_id)
            batch_client.pool.delete(pool_id)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.cfg")

    run(config)
