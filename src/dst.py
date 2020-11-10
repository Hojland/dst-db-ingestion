import settings
from typing import List, Dict
import jmespath
import pandas as pd
import re
import asyncio
from aiohttp import ClientSession
import aiohttp
from itertools import compress
from io import StringIO
import logging
from utils import utils


class DST():
    def __init__(self):
        self.base_url = settings.DST_BASE_URL
        self.session = ClientSession()

    async def get_table_info(self, table_id: str, params: dict=None):
        default_params = {
                    "format": "JSON",
                    "lang": "en"
                    }
        url = f"{self.base_url}/tableinfo/{table_id}"
        if not params:
            params = default_params
        res = await self.get(url, params)
        variables = res['variables']
        return variables

    async def get_table(self, table_id: str, variables: List[Dict], params: dict=None, request_type: str='POST', out_format: str='CSV'):
        def special_case_values(variable_values: list, table_variables_values: list):
            if re_obj := re.search('(>|<)(=|)', variable_values[0]):
                operator = re_obj[0]
                boolean_filter = [utils.logical_operator_render(variable_values[0], table_value, operator) for table_value in table_variables_values]
                new_variable_values = list(compress(table_variables_values, boolean_filter))
                #new_variable_values = [int(value) for value in new_variable_values]
            else:
                raise ValueError('a special case variable is a variable, that is using an operator, but the endpoint wont accept operators')
            return new_variable_values

        #default_params = {
        #            "valuePresentation": "Default",
        #            "allowCodeOverrideInColumnNames": "true", # gives english column names coupled with lang en
        #            "lang": "en" # since we can't predefine pivot cols and agg cols this way we choose not to
        #            }
        default_params = {
                    "valuePresentation": "Default",
                    }
        table_info = await self.get_table_info(table_id)
        table_variables, col_names_dct = self.format_table_info(table_info)
        assert all([key in table_variables.keys() for key in variables.keys()]), 'You have provided a variable not available in this table'
        for key, values in variables.items():
            if key == 'Tid':
                continue
            if re.search('(\*)', values[0]):
                pass
            elif re_obj := re.search('(>|<)(=|)', values[0]):
                operator = re_obj[0]
                assert any([utils.logical_operator_render(values[0], table_value, operator) for table_value in table_variables[key]]), f'There is no match for this operator and code value in the values of {key}'
            else:
                assert all([value in table_variables[key] for value in values]), f'You have provided code values not available for key {key}'
        for key in variables.keys():
            if key in ['OMRÅDE', 'BOPOMR']:
                variables[key] = special_case_values(variables[key], table_variables[key])

        if not params:
            params = default_params

        if request_type == 'POST':
            url = f"{self.base_url}/data"
            variables_body = [{'code': k, 'values': v} for k,v in variables.items()]
            params['format'] = out_format
            body = params
            body['table'] = table_id
            body['variables'] = variables_body
            res = await self.post(url, body, return_type='TEXT')

        elif request_type == 'GET':
            url = f"{self.base_url}/data/{table_id}/{out_format}"
            variables_dct = {k:','.join(v) for k,v in variables.items()}
            #variables_str = '&'.join([f"{k}={','.join(v)}" for k,v in variables.items()])
            params = {**params, **variables_dct}
            res = await self.get(url, params, return_type='TEXT')

        df = pd.read_csv(StringIO(res), sep=';')

        # make cols english
        col_names_dct = {k.upper(): v.replace(' ', '_') for k,v in col_names_dct.items()}
        col_names_dct['INDHOLD'] = 'content'
        df.columns = df.columns.str.upper()
        df = df.rename(col_names_dct, axis=1)

        # clean some totals
        for col in list(df):
            if df[col].dtype == 'object':
                df = df.loc[~df[col].str.contains(', total')]
        return df

    def format_table_info(self, table_variables: List[Dict]):
        variables_dct =  {table_variable['id']: jmespath.search('values[].id', table_variable) for table_variable in table_variables}
        col_names_dct = {table_variable['id']: jmespath.search('text', table_variable) for table_variable in table_variables}
        return variables_dct, col_names_dct


# what is stub????? Det er desuden angivet, at variablen område skal placeres i tabellens forspalte (angives som hoved (head) eller forspalte (stub)).
# remember we can query like >=2010K1<=2015K4 
    async def post(self, url: str, body: dict, return_type: str='JSON'):
        res = await self.session.post(url, data=body)
        if res.status != 200:
            message = await res.json()
            raise AssertionError(f"Status for request is {res.status} with reason {res.reason} and message: {message['message']}")
        if return_type == 'JSON':
            try:
                res = await res.json()
            except aiohttp.ContentTypeError:
                import json
                res = await res.text()
                res = json.loads(res)
        elif return_type == 'TEXT':
            res = await res.text()
        return res

    async def get(self, url: str, params: dict, return_type: str='JSON'):
        res = await self.session.get(url, params=params)
        if res.status != 200:
            message = await res.json()
            raise AssertionError(f"Status for request is {res.status} with reason {res.reason} and message: {message['message']}")
        if return_type == 'JSON':
            try:
                res = await res.json()
            except aiohttp.ContentTypeError:
                import json
                res = await res.text()
                res = json.loads(res)
        elif return_type == 'TEXT':
            res = await res.text()
        return res

if __name__ == '__main__':
    dst = DST()
    self = dst
