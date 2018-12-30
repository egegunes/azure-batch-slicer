import datetime
import subprocess


def run():
    print("Task started at {}".format(datetime.datetime.now()))
    subprocess.run(["CuraEngine",
                    "slice",
                    "-j",
                    "/usr/share/cura/resources/definitions/ultimaker2.def.json",
                    "-l",
                    "/home/egegunes/model.stl",
                    "-o",
                    "output.gcode"])
    print("Task finished at {}".format(datetime.datetime.now()))


if __name__ == "__main__":
    run()
