import subprocess

def runCommand(cmd):
    p = subprocess.Popen(
        cmd.split(" "),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    out, err = p.communicate()
    return out.decode("UTF-8"), err.decode("UTF-8")
