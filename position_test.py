#!/usr/bin/env python3

# vi:syntax=python

import sys
import json
import csv
import pandas as pd
from argparse import ArgumentParser, Namespace
from getpass import getpass
from typing import List, Optional
#TO DO, fix the below
from iress.xplan.api import ResourcefulAPICall
from iress.xplan.edai import EDAICall
from iress.xplan.session import Session
from datetime import datetime

def _get_secrets(options: Optional[Namespace]):
    if not options.password:
        options.password = getpass(
            prompt="Password for {user}: ".format(user=options.user_name)
        )
    if not options.otp_secret and options.use_tfa:
        options.otp_secret = getpass(prompt="OTP Secret: ")


def get_arguments(argv: List) -> Optional[Namespace]:
    parser = ArgumentParser()
    parser.add_argument(
        "--base-url",
        "-b",
        help="The Base Xplan URL (e.g. https://edai.xplan.iress.com).",
        required=True,
    )
    parser.add_argument(
        "--client-id", "-i", help="The Xplan API key/Client ID.", required=True
    )
    parser.add_argument(
        "--otp-secret",
        "-o",
        help="The One Time Password (OTP) secret, if not provided the script will prompt the user.",
    )
    parser.add_argument("--user-name", "-u", help="The Xplan user name.", required=True)
    parser.add_argument(
        "--password",
        "-p",
        help="The Xplan user password, if not provided the script will prompt the user.",
    )
    parser.add_argument(
        "--use-tfa", "-tfa", help="Use 2FA for authentication.", action="store_true"
    )

    parser.add_argument(
        "--api-example",
        "-api",
        help="Run the API example with basic authentication.",
        action="store_true",
    )
    parser.add_argument(
        "--edai-example", "-edai", help="Run the EDAI example.", action="store_true"
    )

    known_args, unknown_args = parser.parse_known_args(args=argv)
    _get_secrets(known_args)

    return known_args


def api_example(session: Session):
    client = ResourcefulAPICall(session=session, api_path="entity/client-v4")
    client_content = client.call_content()
    client_content_list = json.loads(client_content)
    all_portfolioid = [{r['id']} for r in client_content_list]
    return all_portfolioid

def client_pos_example(portfolioid,session: Session):
    api_path = "portfolio/portfolio/" + "C" + str(portfolioid) + "/position" #portfolioids are just passed as numbers, we need potentially C or J added to the string, need to work this out.
    client_positions = ResourcefulAPICall(session=session, api_path = api_path)
    client_positions_content = client_positions.call_content()
    return client_positions_content

def all_acct_api_example(session: Session):
    page_index = 0
    all_accountid = []
    while True and page_index < 100:
        #page_limimt is 200 per call, page_index limit is just to stop indefinite running
        api_path = "portfolio/account?page_index=" + str(page_index)
        all_accounts = ResourcefulAPICall(session=session, api_path=api_path)
        all_accounts_content = all_accounts.call_content()
        all_accounts_content_list = json.loads(all_accounts_content)
        temp_all_accountid = [{'accountid':r['accountid'],'ips_account_id':r['ips_account_id']} for r in all_accounts_content_list if r['ips_account_id'] is not None]
        if len(temp_all_accountid):
            page_index += 1
            all_accountid.extend(temp_all_accountid)
        else:
            print(page_index)
            break

    return all_accountid

def acct_pos_example(accountid, session:Session,):
    api_path = "portfolio/account/" + str(accountid) + "/position?fields.0=code&fields.1=position_desc&fields.2=value&fields.3=security.sedol&fields.4=security.description&fields.5=account.tax_structure&fields.6=account.service_type&date=2023-06-30"
    position = ResourcefulAPICall(session=session, api_path=api_path)
    position_content = position.call_content()
    return position_content

def all_position_example(session: Session):
    all_accountid = all_acct_api_example(session=session)
    #print("all_accountid"&all_accountid)
    #all_pos_list = []
    #initialise an empty DataFrame
    df = pd.DataFrame()
    count = 1
    for account in all_accountid: 
        if count < 20000:
            #print(count)
            accountid = account['accountid']
            position_content = acct_pos_example(accountid, session=session)
            position_content_list = json.loads(position_content)
            service_type_bool = True #any(item['account']['service_type'] == "Under Advice" for item in position_content_list)
            count = count +1
            if count % 200 == 0:
                print("Count is:",count,format(float(count/6000)*100,".0f"),"%")

            if service_type_bool:
                #all_pos_list.append(position_content_list)
                temp_df = pd.DataFrame(position_content_list)
                df = pd.concat([df, temp_df], ignore_index=True)
    #print(df)
    return(df)

def edai_example(session: Session):
    client = EDAICall(session=session)

    print(client.get_value(path=f"entitymgr/user/{session.entity_id}/field/last_name"))
    print(client.get_value(path=f"entitymgr/user/{session.entity_id}/field/first_name"))


def call(session: Session, options: Optional[Namespace]):
    if options.edai_example:
        edai_example(session)
    else:
        #api_example(session)
        all_acct_api_example(session)


if __name__ == "__main__":
    then = datetime.now()
    opts = get_arguments(sys.argv[1:])

    xp_session = Session(opts.base_url, client_id=opts.client_id)
    xp_session.authenticate(
        user=opts.user_name, pwd=opts.password, otp_secret=opts.otp_secret
    )
    #call(xp_session, opts)
    df = all_position_example(xp_session)
    df["_val"] = pd.to_numeric(df["value"].apply(lambda x: x["_val"]))
    df['_code'] = df.apply(lambda row: row['security']['sedol'] if row['security']['sedol'] != '' else row['code'], axis=1)
    df['_description'] = df.apply(lambda row: row['security']['description'], axis=1)
    print(df)
    df.to_excel("data.xlsx",index = False)
    sum_by_code = df.groupby('_description')["_val"].sum().reset_index()
    sum_by_code = sum_by_code.sort_values("_val", ascending=False)
    sum_by_code.to_excel("Investment Holding Report.xlsx", index=False)
    print(sum_by_code)
    print("Complete")
    now = datetime.now()
    print(now - then)