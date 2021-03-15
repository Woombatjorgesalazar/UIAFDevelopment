import subprocess, os , common

def maketreeHDFS(directorio):
    cmd="hadoop fs -mkdir -p "+ directorio
    cmdHFDS(cmd)

def deletefilesHDFS(directorio):
   cmd="hadoop fs -rm -R " + directorio +"*"

   try:
       cmdHFDS(cmd)
   except  subprocess.CalledProcessError  as error:
       if str(error.output).find("No such file or directory") == 0:
           raise

def deletefileHDFS(file):
   cmd="hadoop fs -rm " + file

   try:
       cmdHFDS(cmd)
   except  subprocess.CalledProcessError  as error:
       if str(error.output).find("No such file or directory") == 0:
           raise

def copyFromLocal (origenL, desHFS):

    try:
        cmd="hadoop fs -copyFromLocal   %s %s"%(origenL,desHFS)
        cmdHFDS(cmd)
    except subprocess.CalledProcessError  as error:
        if str(error.output).find("No such file or directory") > 0:
            maketreeHDFS(desHFS)
            cmdHFDS(cmd)
        elif str(error.output).find("File exists") > 0:
            return
        else:
            raise


def putfilesHDFS (localOrineg,HDFSdirectorio, ext="txt"):
    cmd ="hadoop fs -put " + localOrineg + '*.' + ext + ' ' + HDFSdirectorio
    try:
        cmdHFDS(cmd)
    except  subprocess.CalledProcessError  as error:
        if str(error.output).find("No such file or directory")>0:
            maketreeHDFS(HDFSdirectorio)
            cmdHFDS(cmd)
        elif str(error.output).find("File exists")>0:
            deletefilesHDFS(HDFSdirectorio)
            cmdHFDS(cmd)
        elif str(error.output).find("returned non-zero exit status 1")>0:
            common.execute_cmd ("chmod 776 " + localOrineg + "*.*")
            cmdHFDS(cmd)
        else:
            print (error.output)
            raise

def cmdHFDS (cmd):
    common.verbose_c(cmd,4)
    if not common.GLOBAL_CONFIG["local"]:
       if str(os.getenv('HADOOP_CONF_DIR')).find("hive") > 0:
           os.environ['HADOOP_CONF_DIR'] = os.getenv('SPARK_CONF_DIR') + '/yarn-conf'
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    except  subprocess.CalledProcessError  as error:
        common.verbose_c(error.output,2)
        raise

