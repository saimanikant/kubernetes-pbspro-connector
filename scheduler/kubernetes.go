/*
 * Copyright (C) 1994-2019 Altair Engineering, Inc.
 * For more information, contact Altair at www.altair.com.
 *
 * This file is part of the PBS Professional ("PBS Pro") software.
 *
 * Open Source License Information:
 *
 * PBS Pro is free software. You can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Commercial License Information:
 *
 * For a copy of the commercial license terms and conditions,
 * go to: (http://www.pbspro.com/UserArea/agreement.html)
 * or contact the Altair Legal Department.
 *
 * Altair’s dual-license business model allows companies, individuals, and
 * organizations to create proprietary derivative works of PBS Pro and
 * distribute them - whether embedded or bundled with other software -
 * under a commercial license agreement.
 *
 * Use of Altair’s trademarks, including but not limited to "PBS™",
 * "PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's
 * trademark licensing policies.
 *
 */

package main

import (
	"bytes"
	"encoding/json"
	"errors"	
	"io/ioutil"
	"log"
	"fmt"
	"os/exec"         
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"crypto/tls"
)

type PBSPodList struct {
	Items []PBSPod `json:"items"`
}

type PBSPod struct {
	Metadata PBSPodMetadata `json:"metadata"`
}

type PBSPodMetadata struct {
	Name        string            `json:"name,omitempty"`
	Annotations map[string]string `json:"annotations"`
}


var (
	kubectl_path     = "/usr/bin/kubectl"
	qsub_path        = "/opt/pbs/bin/qsub"
	qstat_path       = "/opt/pbs/bin/qstat"
	sched_name       = "PBS_custom_sched"
	queue_name       = "reservationK8"


	bindingEndpoint  = "/api/v1/namespaces/%s/pods/%s/binding/"
	eventEndpoint    = "/api/v1/namespaces/%s/events"
	nodeEndpoint     = "/api/v1/nodes"
	podEndpoint      = "/api/v1/pods"
	podNamespace	 = "/api/v1/namespaces/%s/pods/"
	watchPodEndpoint = "/api/v1/watch/pods"
)

func postsEvent(event Event, apiserver string, token string, pod_namespace string) error {
	var bf []byte
	body := bytes.NewBuffer(bf)
	error := json.NewEncoder(body).Encode(event)
	if error != nil {
		return error
	}

	req :=  &http.Request{
		Body:          ioutil.NopCloser(body),
		ContentLength: int64(body.Len()),
		Header:        make(http.Header),
		Method:        http.MethodPost,
		URL: &url.URL{
			Host:   apiserver,
			Path:   fmt.Sprintf(eventEndpoint, pod_namespace),
			Scheme: "https",
		},
	}
	req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Authorization", "Bearer " + token)

	tr := &http.Transport{
        	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        client := &http.Client{Transport: tr}

	res, error := client.Do(req)
	if error != nil {
		return error
	}
	if res.StatusCode != 201 {
		b, _ := ioutil.ReadAll(res.Body)
		log.Println(string(b))
		return errors.New("Event: Unexpected HTTP status code" + res.Status)
	}
	return nil
}

func watchUnscheduledPods(apiserver string, token string) (<-chan Pod, <-chan error) {	
	pods := make(chan Pod)
	errc := make(chan error, 1)

	val := url.Values{}
	val.Set("fieldSelector", "spec.nodeName=")
	val.Add("sort","creationTimestamp asc")
	req  := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:     apiserver,
			Path:     watchPodEndpoint,
			RawQuery: val.Encode(),
			Scheme:   "https",
		},
	}	
	req.Header.Set("Accept", "application/json, */*")
	req.Header.Set("Authorization", "Bearer " + token)

	tr := &http.Transport{
        	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        client := &http.Client{Transport: tr}

	go func() {		
		for {			
			res, error := client.Do(req)
			if error != nil {
				errc <- error
				time.Sleep(5 * time.Second)
				continue
			}

			if res.StatusCode != 200 {
				errc <- errors.New("Error code: " + res.Status)
				b, _ := ioutil.ReadAll(res.Body)
				log.Println(string(b))
				time.Sleep(5 * time.Second)
				continue
			}

			decoder := json.NewDecoder(res.Body)
			for {
				var event PodWatchEvent
				error = decoder.Decode(&event)
				if error != nil {
					errc <- error
					break
				}

				if event.Type == "ADDED" {
					pods <- event.Object					
				}
			}
		}
	}()
	return pods, errc
}

func getUnscheduledPods(apiserver string, token string) (*PodList, error) {
	var podList PodList	

	val := url.Values{}
	val.Set("fieldSelector", "spec.nodeName=")

	req  := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:     apiserver,
			Path:     podEndpoint,
			RawQuery: val.Encode(),
			Scheme:   "https",
		},
	}
	req.Header.Set("Accept", "application/json, */*")
        req.Header.Set("Authorization", "Bearer " + token)

	tr := &http.Transport{
        	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        client := &http.Client{Transport: tr}

	res, error := client.Do(req)
	if error != nil {
		return nil, error
	}
	error = json.NewDecoder(res.Body).Decode(&podList)
	if error != nil {
		return nil, error
	}		
	return &podList, nil
}


func fit(pod *Pod, apiserver string, token string) (string,error) {
	
	spaceRequired := 0
	memoryRequired := 0
	flag := 0

	ns, err := exec.Command(kubectl_path, "get", "ns", "-l", sched_name + "=true", "-o", "name").Output()
	if err != nil {
		log.Println(err)
        }
	ns_list := strings.Split(string(ns), "\n")
	ns_list = ns_list[:len(ns_list)-1]

        // check if pod belongs in the list of supported namespaces
        for _,item := range ns_list {
		item := strings.Split(item, "/")[1]
		if item == pod.Metadata.NameSpace {
			flag = 1
		}
	}
	if flag == 0 {
                log.Println("Pod " + pod.Metadata.Name + " part of " + pod.Metadata.NameSpace + " namespace. Skip scheduling")
		return "",nil
	}
	// check the schedulerName of the pod
	if sched_name != pod.Spec.SchedulerName {
		log.Println("Skip scheduling Pod " + pod.Metadata.Name)
		return "",nil
	}
	jobid := ""
	if pod.Metadata.Annotations["JobID"] == "" {

		//calculate resources

		for _, c := range pod.Spec.Containers {
			cpu_req := c.Resources.Requests["cpu"]
			if strings.HasSuffix(cpu_req, "m") {
				cpu_req = strings.TrimSuffix(cpu_req, "m")
				cores, err := strconv.Atoi(cpu_req)
                                if err != nil {
                                        return "Error",err
                                }
				if cores > 1000 {
					cores = cores/1000
				} else if cores < 1000 && cores > 0{
					cores = 1
				}
				spaceRequired += cores
				continue
			}
			if cpu_req != "" {
				cores, err := strconv.Atoi(cpu_req)
				if err != nil {
                        		return "Error",err
                        	}
				spaceRequired += cores
			}
		}
		if spaceRequired == 0 {
			spaceRequired = 1
		}
		ncpus := strconv.Itoa(spaceRequired)

		suffix := "MB"
		mem_req := ""
		for _, c := range pod.Spec.Containers {
			if strings.HasSuffix(c.Resources.Requests["memory"], "Mi") {
				suffix = "MB"
				mem_req = strings.TrimSuffix(c.Resources.Requests["memory"], "Mi")
			}
			if strings.HasSuffix(c.Resources.Requests["memory"], "Gi") {
                                suffix = "GB"
                                mem_req = strings.TrimSuffix(c.Resources.Requests["memory"], "Gi")
                        }
			if strings.HasSuffix(c.Resources.Requests["memory"], "Ki") {
                                suffix = "KB"
                                mem_req = strings.TrimSuffix(c.Resources.Requests["memory"], "Ki")
                        }
			if mem_req != "" {
				mem, err1 := strconv.Atoi(mem_req)
				if err1 != nil {
					return "Error",err1
				}
				memoryRequired += mem
			}
		}
		if memoryRequired == 0 {
			memoryRequired = 1
		}
		mem := strconv.Itoa(memoryRequired)
		mem = mem + suffix

		argstr := qsub_path + " -l select=1:ncpus=" + ncpus + ":mem=" + mem + " -N " +pod.Metadata.Name + " -q " + queue_name + " -vPODNAME=" + pod.Metadata.Name + ",PODNS=" + pod.Metadata.NameSpace + " kubernetes_job.sh"
		log.Println(argstr)
		out, err := exec.Command("bash", "-c", argstr).Output()
	        if err != nil {
		    log.Println("qsub failed")
	            log.Println(err)
        	}
        	jobid = string(out)
		last := len(jobid) - 1
		jobid = jobid[0:last]
        	time.Sleep(5000 * time.Millisecond)
		
		// Store jobid in pod
		annotation(pod, jobid, apiserver, token)	
							    
	} else {				
		jobid = pod.Metadata.Annotations["JobID"] 						
	}
	// find a node
	nodename := findnode(jobid)

	if nodename != "" {
		log.Println("Job Scheduled, associating node " + nodename + " to " + pod.Metadata.Name)
		return nodename, nil
	} 
	log.Println("PBS job not running, looking for comment in qstat -f")
	out1, err := exec.Command("bash", "-c" ,qstat_path + " -f " + jobid).Output()        
        if err != nil {
            log.Println(err)
        }
	comment := string(out1)
 	splits := strings.Split(comment, "\n")	
	for i, n := range splits{
		if strings.Contains(n, "comment") {
			log.Println(pod.Metadata.Name + ":" + splits[i])
			break;
            }
            i++;
        }	

	timestamp := time.Now().UTC().Format(time.RFC3339)
	event := Event{
		Count:          1,
		Message:        fmt.Sprintf("pod (%s) failed to fit in any node\n", pod.Metadata.Name),
		Metadata:       Metadata{GenerateName: pod.Metadata.Name + "-"},
		Reason:         "FailedScheduling",
		LastTimestamp:  timestamp,
		FirstTimestamp: timestamp,
		Type:           "Warning",
		Source:         EventSource{Component: "PBS-scheduler"},
		InvolvedObject: ObjectReference{
			Kind:      "Pod",
			Name:      pod.Metadata.Name,
			Namespace: pod.Metadata.NameSpace,
			Uid:       pod.Metadata.Uid,
		},
	}
	postsEvent(event, apiserver, token, pod.Metadata.NameSpace)
	return "",nil
	
	
}


func findnode(jobid string) string {

	returnstring := ""

        out1, err := exec.Command("bash", "-c" , qstat_path + " -f " + jobid).Output()        
        if err != nil {
		log.Println("qstat failed")
		log.Println(err)
        }
	nodevalue := string(out1)
 	splits := strings.Split(nodevalue, " ")	
	flag1 := "job_state"
	flag2 := "substate"
	i := 0
	for i >= 0{
            if splits[i] == flag1 {
                break;
            }
            i++;
        }
	
	j := 0
	for j >= 0{
            if splits[j] == flag2 {
                break;
            }
            j++;
        }
	job_state := splits[i+2]
	last1 := len(job_state) - 1		

	substate := splits[j+2]
	last2 := len(substate) - 1	
	
	if job_state[0:last1] == "R" && substate[0:last2] == "42" {	
	    log.Println("Finding node")
	    word := "exec_host"
            i = 0
            for i >= 0{
                if splits[i] == word {
                    break;
                }
                i++;
            }
	    nodename := splits[i+2]
	    returnstring = strings.SplitAfter(nodename, "/")[0]
	    if returnstring[len(returnstring) - 1:len(returnstring)] == "/" {
	        last := len(returnstring) - 1
	        returnstring = returnstring[0:last]
	    }
	}
	
	return returnstring
}



func annotation(pod *Pod, jobid string, apiserver string, token string) {		
					
	annotations := map[string]string{
		"JobID": jobid,
	}			
	patch := PBSPod{
		PBSPodMetadata{
			Annotations: annotations,
		},
	}
	
	var b []byte
	body := bytes.NewBuffer(b)
	error := json.NewEncoder(body).Encode(patch)
	if error != nil {
		log.Println(error)
	}

	var ns = fmt.Sprintf(podNamespace, pod.Metadata.NameSpace)
	url := "https://" + apiserver + ns + pod.Metadata.Name
	req, error := http.NewRequest("PATCH", url, body)
	if error != nil {
		log.Println(error)
	}
	
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	req.Header.Set("Accept", "application/json, */*")
        req.Header.Set("Authorization", "Bearer " + token)

        tr := &http.Transport{
        	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        client := &http.Client{Transport: tr}
	res, error := client.Do(req)
	if error != nil {
		log.Println(error)
	}					
	if res.StatusCode != 200 {
		b, _ := ioutil.ReadAll(res.Body)
		log.Println(string(b))
		log.Println(error)
	}
	
	log.Println("Associating Jobid " + jobid + " to pod " + pod.Metadata.Name + " in namespace " + pod.Metadata.NameSpace)
}


func bind(pod *Pod, node string, apiserver string, token string) error {
	bindreq := Binding{
		ApiVersion: "v1",
		Kind:       "Binding",
		Metadata:   Metadata{Name: pod.Metadata.Name},
		Target: Target{
			ApiVersion: "v1",
			Kind:       "Node",
			Name:       node,
		},
	}

	var b []byte
	body := bytes.NewBuffer(b)
	error := json.NewEncoder(body).Encode(bindreq)
	if error != nil {
		return error
	}

	req :=  &http.Request{
		Body:          ioutil.NopCloser(body),
		ContentLength: int64(body.Len()),
		Header:        make(http.Header),
		Method:        http.MethodPost,
		URL: &url.URL{
			Host:   apiserver,
			Path:   fmt.Sprintf(bindingEndpoint, pod.Metadata.NameSpace, pod.Metadata.Name),
			Scheme: "https",
		},
	}
	req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Authorization", "Bearer " + token)

	tr := &http.Transport{
        	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        client := &http.Client{Transport: tr}

	res, error := client.Do(req)
	if error != nil {
		return error
	}
	if res.StatusCode != 201 {
		b, _ := ioutil.ReadAll(res.Body)
		log.Println(string(b))
		return errors.New("Binding: Unexpected HTTP status code" + res.Status)		
	}

	// Shoot a Kubernetes event that the Pod was scheduled successfully.
	msg := fmt.Sprintf("Successfully assigned %s to %s in namespace %s", pod.Metadata.Name, node, pod.Metadata.NameSpace)
	timestamp := time.Now().UTC().Format(time.RFC3339)
	event := Event{
		Count:          1,
		Message:        msg,
		Metadata:       Metadata{GenerateName: pod.Metadata.Name + "-"},
		Reason:         "Scheduled",
		LastTimestamp:  timestamp,
		FirstTimestamp: timestamp,
		Type:           "Normal",
		Source:         EventSource{Component: "PBS-scheduler"},
		InvolvedObject: ObjectReference{
			Kind:      "Pod",
			Name:      pod.Metadata.Name,
			Namespace: pod.Metadata.NameSpace,
			Uid:       pod.Metadata.Uid,
		},
	}
	log.Println(msg)
	return postsEvent(event, apiserver, token, pod.Metadata.NameSpace)
}
