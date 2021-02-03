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
	"log"
	"sync"
	"time"
)

var processLock = &sync.Mutex{}

func resolveUnscheduledPods(interval int, done chan struct{}, apiserver string, token string, wg *sync.WaitGroup) {			
	for {
		log.Println("\nStarting Scheduler Iteration")
		select {
		case <-time.After(time.Duration(interval) * time.Second):							
			err := reschedulePod(apiserver, token)
			if err != nil {
				log.Println(err)
			}
			log.Println("End of Iteration")
		case <-done:
			wg.Done()
			log.Println("Stopped reconciliation loop.")
			return
		}
	}
}

func trackUnscheduledPods(done chan struct{}, apiserver string, token string, wg *sync.WaitGroup) {	
	pods, errc := watchUnscheduledPods(apiserver, token)

	for {		
		select {
		case err := <-errc:
			log.Println(err)
		case pod := <-pods:
			processLock.Lock()
			time.Sleep(2 * time.Second)			
			err := schedulePod(&pod, apiserver, token)
			if err != nil {
				log.Println(err)
			}
			processLock.Unlock()
		case <-done:
			wg.Done()
			log.Println("Stopped scheduler.")
			return
		}
	}
}

func schedulePod(pod *Pod, apiserver string, token string) error {	
	nodevalue,err := fit(pod, apiserver, token)
	if err != nil {
		return err
	}
	if nodevalue == "" {
		return nil
	}
	err = bind(pod, nodevalue, apiserver, token)
	if err != nil {
		return err
	}	
	return nil
}

func reschedulePod(apiserver string, token string) error {
	processLock.Lock()
	defer processLock.Unlock()
	pods, err := getUnscheduledPods(apiserver, token)
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {		
		err := schedulePod(&pod, apiserver, token)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}
