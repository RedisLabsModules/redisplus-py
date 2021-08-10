from invoke import task
import shutil
import tox


@task
def dockerstart(c):
    """Starts the docker used in the project"""
    r = tox.config.parseconfig(open("tox.ini").read())
    docker = r._docker_container_configs["redismodtox"]["image"].replace('"', "")
    cmd = "docker run -p 6379:6379 {}".format(docker)
    c.run(cmd)


@task
def build_docs(c, frm="html"):
    """Build docs"""
    shutil.rmtree("dist/docs")
    c.run("rm -rf docs/redisplus* docs/modules.rst")
    c.run("sphinx-apidoc -o docs src/redisplus")
    c.run("rm -rf dist/docs")
    c.run("sphinx-build -M {} docs dist/docs".format(frm))

@task(pre=[build_docs])
def docserver(c, port=8000):
    """Run a webserver to browse the docs"""
    docdir = "dist/docs/html"

    with c.cd(docdir):
        print("Starting webserver on port {}.".format(port))
        try:
            c.run("python3 -m http.server {}".format(port))
        except KeyboardInterrupt():
            pass
