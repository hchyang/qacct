#!/usr/bin/env python3

#########################################################################
# Author: Hechuan Yang
# Created Time: 2019-01-30 13:58:40
# File Name: qacct.py
# Description: 
#########################################################################

import os
import sys
import argparse
import datetime
import subprocess

def main():
    parser=argparse.ArgumentParser(
        description='Retrieve the records of killed jobs on big cluster')
    p = subprocess.Popen("whoami", stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    default=output.decode().rstrip()
    parser.add_argument('-q','--query',type=str,default=default,
        help='the string you want to query [{}]'.format(default))
    default=3
    parser.add_argument('-d','--days',type=int,default=default,
        help='find all the matching records in last INT days [{}]'.format(default))
    parser.add_argument('-l','--last',action='store_true',
        help='print the last matching record, not all of them')
    args=parser.parse_args()

    logpath='/pnas/spool/hsd_logs'
    today=datetime.date.today()
    for i in range(args.days):
        date=today-datetime.timedelta(days=i)
        log='{}/{}'.format(logpath,date.strftime('%Y%m%d'))
        found=False
        block=[]
        with subprocess.Popen(['tac',log],stdout=subprocess.PIPE) as proc:
            for line in proc.stdout:
                line=line.rstrip().decode()
                if line.endswith('end----------------------------------------------'):
                    block=[]
                    found=False
                block.append(line)
                if args.query in line:
                    found=True
                if line.startswith('---------------<') and found:
                    for i in block[::-1]:
                        print(i)
                    if args.last:
                        break
        if found:
            if args.last:
                break

if __name__=='__main__':
    main()
