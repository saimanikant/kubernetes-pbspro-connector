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
	"os"
	"os/signal"
	"sync"
	"syscall"
	"os/exec"        
	"fmt"
	"encoding/base64"
)


func main() {	
 
	apiserver, err := exec.Command("/usr/bin/kubectl", "config", "view", "--minify", "-o", "jsonpath='{.clusters[0].cluster.server}'").Output()
        if err != nil {
        	log.Printf("%s", err)
    	}
	apiserver = apiserver[9 : len(apiserver)-1]         
        fmt.Printf("API server: %s\n", apiserver)

	serviceacc := "default"

        secret, err := exec.Command("/usr/bin/kubectl", "get", "serviceaccount", serviceacc, "-o", "jsonpath='{.secrets[0].name}'").Output()
        if err != nil {
                log.Printf("%s", err)
        }
        secret = secret[1 : len(secret)-1]
        fmt.Printf("Service account: %s\n", secret)

        token_en, err := exec.Command("/usr/bin/kubectl", "get", "secret", string(secret), "-o", "jsonpath='{.data.token}'").Output()
        if err != nil {
                log.Printf("error %s\n", err)
        }

	token_de, err := base64.StdEncoding.DecodeString(string(token_en[1 : len(token_en)-1]))
        if err != nil {
                log.Fatal("error:", err)
        }


	channel := make(chan struct{})
	var wait sync.WaitGroup

	wait.Add(1)
	go trackUnscheduledPods(channel, string(apiserver), string(token_de), &wait)

	wait.Add(1)
	go resolveUnscheduledPods(20, channel, string(apiserver), string(token_de), &wait)

	signalch := make(chan os.Signal, 1)
	signal.Notify(signalch, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalch:
			log.Printf("Shutdown signal received, exiting...")
			close(channel)
			wait.Wait()
			os.Exit(0)
		}
	}
}
