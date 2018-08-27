from reactors.utils import Reactor, agaveutils
import copy
import json


def submit(r,system,path):
    ag = r.client
    job_def = copy.copy(r.settings.bismark)
    inputs = job_def["inputs"]
    inputs["fastq1"] = "agave://"+system+"/"+path
    inputs["genome_folder"] = "agave://tacc-globalfs-urrutia/bismark/genome"
    job_def.inputs = inputs
    job_def.archiveSystem = system
    dir=path.split('/')[:-1]
    dir='/'.join(dir)
    job_def.archivePath = dir + '/analyzed/'

    try:
        job_id = ag.jobs.submit(body=job_def)['id']
        print(json.dumps(job_def, indent=4))
    except Exception as e:
        print(json.dumps(job_def, indent=4))
        print("Error submitting job: {}".format(e))
        print e.response.content
        return
    return


def main():
    """Main function"""
    r = Reactor()
    r.logger.info("Hello this is actor {}".format(r.uid))
    context = r.context
    print(context)
    file = context.message_dict.file
    print(file)
    system = file.get('systemId')
    path = file.get('path')
    print(system)
    print(path)

    if path.split('/')[-1] == 'analyzed':
        print("not reacting to the analyzed directory upload")
        sys.exit
    else:
        submit(r,system,path)



if __name__ == '__main__':
    main()
