# Kubernetes Connector for PBS Professional

Integration of PBS Professional with Kubernetes for PBS Pro to provision and schedule jobs along with docker containers. This integration  will benefit sites in being able to run both HPC workloads as well as container workloads on the same HPC cluster without needing for partitioning into two separate portions. This integration will also allow sites to take advantage of the sophisticated scheduling algorithms in PBS Pro and administer the cluster centrally using a single scheduler with a global set of policies. Kubernetes ships with a default scheduler but since the default scheduler does not suit our needs, a custom scheduler which talks to the server, getting unscheduled pods, then talking to PBS Pro for scheduling them. This custom scheduler can run instead of, or alongside the default scheduler. User can instruct Kubernetes which scheduler to use in the pod definition. This Integration is also achieved using PBS Pro hooks.  
A hook is a block of Python code that PBS Pro executes at certain events, for example, when a job is queued. Each hook can accept (allow) or reject (prevent) the action that triggers it. A hook can make calls to functions external to PBS Pro.

## How to Setup

Make sure you have a  cluster of nodes where, both [Kubernetes](https://github.com/kubernetes/kubernetes) and [PBS Professional](https://github.com/PBSPro/pbspro) are setup. 
Enter the value for `apiHost`, the location of api-server, in kubernetes.go file.
Setup kubelet to a watched directory by using the --config option. The value to --config is the directory that the kubelet will watch for pod manifests to run. First create a directory for this, and then start the kubelet.  
To Build the custom scheduler, clone the kubernetes-pbspro-connector repository and move into the scheduler folder of kubernetes-pbspro-connector and run the following command:
> go build -a --ldflags '-extldflags "-static"' -tags netgo -installsuffix netgo .  

The above command creates the scheduler binary in the current directory.

Create a hook in PBS Pro for execjob_launch and execjob_end hook events by importing the hook script pbs_kubernetes.PY and the configuration file pbs_kubernetes.CF present in the root folder of kubernetes-pbspro-connector. Add the value of --config passed to kubelet to "kubelet_config" in pbs_kubernetes.CF file.
Following is a example of the hook created in PBS Pro:  
> Hook pbs-kubernetes  
    type = site  
    enabled = true  
    event = execjob_end,execjob_launch  
    user = pbsadmin  
    alarm = 30  
    order = 1  
    debug = false  
    fail_action = none  

### Create a Pod

An example is shown below:

> cat redis.yaml 

<pre>
apiVersion: v1
kind: Pod
metadata:
  name: redis
spec:
  containers:
  - name: redis
    image: redis:latest
    resources:
      requests:
        memory: "640Mi"
        cpu: "1"
      limits:
        memory: "700Mi"
        cpu: "3"  
</pre>

> kubectl create -f redis.yaml
<pre>

pod "redis" created  
</pre>

### Run the Scheduler

Run the custom scheduler as root:

> ./scheduler  
<pre>
starting Scheduler Iteration
2019/04/29 05:52:34 Associating Jobid 5.server to pod redis
2019/04/29 05:52:34 Finding node
2019/04/29 05:52:34 Job Scheduled, associating node node1 to redis
2019/04/29 05:52:34 Successfully assigned redis to node1
2019/04/29 05:52:34 End of Iteration  
</pre>

The pod would have been deployed to the node where the PBS Pro job with same resource requests is. The redis pod should be in running state:
> kubectl get pods -o wide  
<pre>
NAME                           READY     STATUS    RESTARTS   AGE       IP           NODE  
redis                          1/1       Running   0          1m        172.30.9.2   node1
</pre>

The PBS Pro job status will show a job running, whose name matches with the one of the pod.

> qstat -s

<pre>
                                                            Req'd  Req'd   Elap
Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time
--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----
5.server        root     workq    redis       47093   1   1  640mb   --  R 00:02
   Job run at Mon Apr 29 at 07:11 on (node1:ncpus=1:mem=655360kb)
</pre>
