from reactors.utils import Reactor, agaveutils
from agavedb import AgaveKeyValStore
from nonce import *
import copy
import json
import urllib
import yaml
import sys

queueKey = 'datasetQueue'
doneKey = 'doneQueue'

def getDBState(r, key, canBeNone=False):
    ag = r.client
    db = AgaveKeyValStore(ag)

    try:
        stateYaml = db.get(key)
    except Exception as e:
        print(e)
        stateYaml = None

    print stateYaml
    if stateYaml is None:
        if canBeNone:
            r.logger.info("State is not found for " + key +
                                " creating new one")
            state = {}
            stateYaml = ""
        else:
            raise ValueError("State  " + key +
                             " cannot be none...intervetion is needed")
    else:
        state = yaml.load(stateYaml)
    return state

def setDBState(r, key, state):
    assert state is not None, "State saved to DB cannot be None"
    assert key is not None, "KeyName for state preservation \
                                   cannot be None"
    ag = r.client
    db = AgaveKeyValStore(ag)

    stateYaml = yaml.dump(state, allow_unicode=False, encoding='utf-8')
    print("SET DBS Queue")
    print(key)
    db.set(key, stateYaml)

#def addNonceCallback(r, ag, job_def, lastAction, archivePath, archiveSystem):

def addNonceCallback(r, job_def, lastAction, dataset, queues, archiveSystem,
                     archivePath, manifestURL, genomeReference=None):
    assert job_def is not None, "addNonceCallback: job_def is None"
    assert lastAction is not None, "addNonceCallback: lastAction is None"
    assert dataset is not None, "addNonceCallback: dataset is None"
    assert queues is not None, "addNonceCallback: queues is None"
    assert archivePath is not None, "addNonceCallback: archivePath is None"
    assert archiveSystem is not None, "addNonceCallback: archiveSystem is None"
    assert manifestURL is not None, "addNonceCallback: manifestURL is None"

    queues = str(queues)
    reactorUrl = (get_nonce_url(r) + "&lastAction="
                  + urllib.quote_plus(lastAction) + "&dataset="
                  + urllib.quote_plus(dataset)
                  + "&queues=" + urllib.quote_plus(queues)
                  + "&archivePath="+archivePath
                  + "&archiveSystem="+archiveSystem
                  + "&manifestURL="+manifestURL)
    if genomeReference is not None:
        reactorUrl = reactorUrl + "&genomeReference=" \
                     + urllib.quote_plus(genomeReference)
    notifications = job_def["notifications"]
    notif = []
    # reactorUrl = (get_nonce_url(r) + "&lastAction="
    #               + urllib.quote_plus(lastAction)
    #               + "&archivePath="+archivePath
    #               + "&archiveSystem="+archiveSystem)

    note = {"url": reactorUrl, "event": "FINISHED"}
    notif = [note]
    if notifications is not None:
        for item in notifications:
            notif.append(item)

    job_def["notifications"] = notif


def parseCallback(r, ag):
    context = r.context
    assert ag is not None, "parseCallback: Agave client is not present"
    lastAction = urllib.unquote_plus(context.lastAction)
    queues = urllib.unquote_plus(context.queues)
    dataset = urllib.unquote_plus(context.dataset)
    archivePath = urllib.unquote_plus(context.archivePath)
    archiveSystem = urllib.unquote_plus(context.archiveSystem)
    manifestURL = urllib.unquote_plus(context.manifestURL)
    assert lastAction is not None, "parseNonceCallback: lastAction is None"
    assert dataset is not None, "addNonceCallback: dataset is None"
    assert queues is not None, "parseNonceCallback: queues is None"
    assert archivePath is not None, "parseNonceCallback: archivePath is None"
    assert archiveSystem is not None, \
        "parseNonceCallback: archiveSystem is None"
    assert manifestURL is not None, "parseNonceCallback: manifestURL is None"
    try:
        genomeReference = urllib.unquote_plus(context.genomeReference)
    except Exception:
        genomeReference = None

    return (lastAction, dataset, queues, archivePath, archiveSystem,
            manifestURL, genomeReference)


def manifest(r,ag,m):
    db = AgaveKeyValStore(ag)
    file = m.file
    archiveSystem = file.get('systemId')
    path= file.get('path')
    manifest_file_name = path.split('/')[-1]
    if manifest_file_name.split('.')[-1] != 'json':
        print("Uploaded file does not have .json suffix, File:", path)
        sys.exit(0)
    # must send double-notifications for now, this sets agavedb for the notification
    # and only executes on the second notification
    state = getDBState(r, manifest_file_name, canBeNone=True)
    if state == {}:
        setDBState(r, manifest_file_name, "done")
        sys.exit(0)
    if state == "done":
        db.rem(manifest_file_name)

        agaveutils.agave_download_file(agaveClient=ag,
                                       agaveAbsolutePath=path,
                                       systemId=archiveSystem,
                                       localFilename=manifest_file_name)

        manifest = json.load(open(manifest_file_name))
        for comparison in manifest:
            sample1 = manifest[comparison]["sample1"]
            sample2 = manifest[comparison]["sample2"]
            genomeReference = manifest[comparison]["genome"]
            s1name = sample1.split('/')[-1].split('.')[0]
            s2name = sample2.split('/')[-1].split('.')[0]
            queue = comparison + s1name + s2name
            state = getDBState(r, queue, canBeNone=True)
            state[queueKey] = [str(s1name),str(s2name)]
            state[doneKey] = []
            manifestURL = "agave://" + archiveSystem + "/" + path
            setDBState(r,queue,state)
            genomeprep(r,ag,path,queue,archiveSystem,sample1,sample2,genomeReference,manifestURL)



def genomeprep(r,ag,path,queue,archiveSystem,sample1,sample2,genomeReference,manifestURL):
    job_def = copy.copy(r.settings.genomeprep)
    inputs = job_def["inputs"]
    inputs["fasta"] = genomeReference
    job_def.inputs = inputs
    archivePath = "/".join(path.split('/')[0:-1]) + "/analyzed/"
    job_def.archivePath = archivePath + "genome/"
    job_def.archiveSystem = archiveSystem
    dataset = sample1 + ',' + sample2
    queues = queue
    addNonceCallback(r, job_def, "genomeprep", dataset, queues,
                     archiveSystem=archiveSystem, archivePath=archivePath,
                     manifestURL=manifestURL, genomeReference=genomeReference)

    try:
        job_id = ag.jobs.submit(body=job_def)['id']
        print(json.dumps(job_def, indent=4))
    except Exception as e:
        print(json.dumps(job_def, indent=4))
        print("Error submitting job: {}".format(e))
        print e.response.content
        return
    return


def bismark(r,ag,m):
    (lastAction, dataset, queues, archivePath, archiveSystem,
        manifestURL, genomeReference) = parseCallback(r, ag)

    samples = dataset.split(",")

    for sample in samples:
        job_def = copy.copy(r.settings.bismark)
        sample_name = sample.split('/')[-1].split('.')[0]
        inputs = job_def["inputs"]
        inputs["fastq1"] = sample
        inputs["genome_folder"] = "agave://" + archiveSystem + "/" + archivePath + "/genome"
        job_def.inputs = inputs
        job_def.archiveSystem = archiveSystem
        job_def.archivePath = archivePath + "/bismark"

        addNonceCallback(r, job_def, "bismark", sample_name, queues,
                         archiveSystem=archiveSystem, archivePath=archivePath,
                         manifestURL=manifestURL, genomeReference=genomeReference)

        try:
            job_id = ag.jobs.submit(body=job_def)['id']
            print(json.dumps(job_def, indent=4))
        except Exception as e:
            print(json.dumps(job_def, indent=4))
            print("Error submitting job: {}".format(e))
            print e.response.content
            return
        return


def bme(r,ag,m):
    (lastAction, dataset, queues, archivePath, archiveSystem,
        manifestURL, genomeReference) = parseCallback(r, ag)

    r1 = json.dumps(m["inputs"]["fastq1"]).split('/')[-1].split('.')[0]



    print("r1:",r1, "genome_folder:",genome_folder)



    job_def = copy.copy(r.settings.bme)
    inputs = job_def["inputs"]
    inputs["bam"] = "agave://"+archiveSystem+"/"+archivePath+'/bismark/'+ r1 +'_bismark_bt2.bam'
    inputs["fasta"] = genomeReference
    job_def.inputs = inputs
    job_def.archiveSystem = archiveSystem
    job_def.archivePath = archivePath + '/bme/'

    addNonceCallback(r, job_def, "bme", dataset, queues,
                     archiveSystem=archiveSystem, archivePath=archivePath,
                     manifestURL=manifestURL, genomeReference=genomeReference)

    try:
        job_id = ag.jobs.submit(body=job_def)['id']
        print(json.dumps(job_def, indent=4))
    except Exception as e:
        print(json.dumps(job_def, indent=4))
        print("Error submitting job: {}".format(e))
        print e.response.content
        return
    return


def bisukit(r,ag,m):
    print("reactorCONTEXT", r.context)
    (lastAction, dataset, queues, archivePath, archiveSystem,
        manifestURL, genomeReference) = parseCallback(r,ag)

    # First we check the DB state, moves over samples that have been processed,
    # and proceed when all samples have processed

    state = getDBState(r,queues)
    queued = state[queueKey]

    try:
        done = state[doneKey]
    except Exception as e:
        r.logger.info("Done is not present for " + queue +
                      " initalizing")
        done = []
        state[doneKey] = done

    assert dataset in queued or done, ("Dataset " + dataset +
                                       " sent to bundle but not in queue: "
                                       + queueKey + " or done:" + doneKey)
    if dataset in queued:
        state[queueKey].remove(dataset)

    if dataset not in done:
        state[doneKey].append(dataset)
    setDBState(r, queues, state)


    # if all samples have process, submits job:
    if len(queued) == 0:
        job_def = copy.copy(r.settings.bisulkit)
        inputs = job_def["inputs"]
        sample1 = state[doneKey][0]
        sample2 = state[doneKey][1]
        input_path = "agave://" + archiveSystem + "/" + archivePath + "/bme/"
        inputs["ot1"] = input_path + "CpG_OT_" + sample1 + "_bismark_bt2.txt"
        inputs["ot2"] = input_path + "CpG_OT_" + sample2 + "_bismark_bt2.txt"
        inputs["ob1"] = input_path + "CpG_OB_" + sample1 + "_bismark_bt2.txt"
        inputs["ob2"] = input_path + "CpG_OB_" + sample2 + "_bismark_bt2.txt"
        #inputs["ctot1"] = input_path + "CpG_CTOT_" + sample1 + "_bismark_bt2.txt"
        inputs["genome"] = genomeReference
        parameters = job_def["parameters"]
        parameters["input1"] = sample1
        parameters["input2"] = sample2
        job_def.inputs = inputs
        job_def.archiveSystem = archiveSystem
        job_def.archivePath = archivePath + '/bisukit/'
        job_def.parameters = parameters
        try:
            job_id = ag.jobs.submit(body=job_def)['id']
            print(json.dumps(job_def, indent=4))
        except Exception as e:
            print(json.dumps(job_def, indent=4))
            print("Error submitting job: {}".format(e))
            print e.response.content
            return
        return

defaultTaskPath = {None: [manifest], "genomeprep": [bismark], "bismark": [bme], "bme": [bisukit]}

def main():
    """Main function"""
    r = Reactor()
    r.logger.info("Hello this is actor {}".format(r.uid))
    ag = r.client
    context = r.context
    print(context)
    m = context.message_dict
    print(m)

    lastAction = None
    try:
        lastAction = context.lastAction
    except Exception as e:
        try:
            lastAction = m.get('lastAction')
        except Exception as e:
            lastAction = None

    r.logger.info("Triggered action for last action of {}".format(lastAction))
    for action in defaultTaskPath[lastAction]:
        if action is not None:
            action(r,ag,m)



if __name__ == '__main__':
    main()
