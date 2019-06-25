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
    parser.add_argument('-q','--query',type=str,required=True,
        help='the string you want to query')
    parser.add_argument('-a','--all',action='store_true',
        help='print all the matching records, not just the last one')
    default=3
    parser.add_argument('-d','--days',type=int,default=default,
        help='find all the matching records in last INT days [{}]'.format(default))
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
                    if not args.all:
                        break
        if found:
            if not args.all:
                break

if __name__=='__main__':
    main()
