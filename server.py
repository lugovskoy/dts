#! /usr/bin/env python
# coding=utf-8

import os
import os.path
import re
import sys
import json
import time
import couchdb
import logging as logger
import cgi
import BaseHTTPServer
import SocketServer
import formatter



table_name = 'patches_un' 
config_path = 'config.json'

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def __get_log(self):
        idx = re.search('/(.+?)\.log', self.path).group(1)
        couch = couchdb.Server('http://10.1.0.35:5984')
        db = couch[table_name]
        if idx not in db:
            return
        doc = db[idx]
        data = db.get_attachment(doc, 'log')
        if data is None:
            data = "Log is not yet created"
        else:
            data = data.read()

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        buf = '<html><meta charset="UTF-8"><body><code>' + data.replace('\n', '<br/>') + '</code></body></html>'
        self.wfile.write(buf)


    def __construct_result_table(self, conf, db_answer=None):
        """
        Construct table with information about current patches.

        :param db_answer: database answer, if None, answer will be written from couchdb server
        :return: HTML formatted table
        """
        #table = '<table class = main>'

        if db_answer is None:
            couch = couchdb.Server('http://10.1.0.35:5984')
            db = couch[table_name]
            docs = [db[idx] for idx in db]
        else:
            docs = db_answer

        # Table header constructed by specification from config.json and header.json
        table = ''

        # Table cells
        for doc in sorted(docs, key=lambda d: d['timestampt'], reverse=True):
            tasks = doc['tasks']
            table += '<table border=1>'

            # Fields from specifications and their args
            for task_spec in conf['tasks']:

                table += '<tr>'
                title = task_spec['title'] if 'title' in task_spec else task_spec['name']
                rowspan = len([p for p in task_spec['params'] if 'visibility' not in p or p['visibility'] != 'hidden'])
                table += '<th rowspan=' + str(rowspan) + '>' + title + '</th>'

                tr_is_open = True
                for param_spec in task_spec['params']:

                    task = tasks[task_spec['name']]

                    if 'visibility' in param_spec and param_spec['visibility'] == 'hidden':
                        continue
                    
                    if not tr_is_open:
                        table += '<tr>'
                    
                    title = param_spec['title'] if 'title' in param_spec else param_spec['name']
                    table += '<th>' + title + '</th>'

                    if 'args' not in task:
                        arg = '&lt;optimized out&gt;'
                    else:
                        arg_name = param_spec['name']
                        arg_type = param_spec['type']
                        arg = task['args'][arg_name] if arg_name in task['args'] else ''

                        if arg_type == 'file':
                            arg = arg['name']
                        elif isinstance(arg, bool):
                            arg = '<span style = "font-size: 85%;">' + (u'●' if arg else u'○') + '</span>'
                        elif isinstance(arg, list):
                            arg = '<br />'.join(map((lambda x : formatter.get_caption(x)), arg))
                        else:
                            arg = formatter.get_caption(str(arg))

                    table += '<td>' + arg + '</td>'
                    table += '</tr>'
                    tr_is_open = False


            # HEAD
            with open(os.path.join(script_path, '..', 'header.json')) as f:
                head = json.load(f)
            
            table += '<tr>'
            table += '<th rowspan=' + str(len(head)) + '>System</th>'

            tr_is_open = True
            for field in head:
                if not tr_is_open:
                    table += '<tr>'
                
                table += '<th>' + field + '</th>'
                arg = '&lt;optimized out&gt;'
                if field == 'log':
                    img_fname = "eye.png"
                    if 'status' in doc:
                        status_icon = os.path.join('imgs', doc['status'].lower() + '.jpg')
                        if os.path.isfile(os.path.join(script_path, status_icon)):
                            img_fname = status_icon
                    arg = '<a href=' + doc['_id'] + '.log><img width="40" height="40" src=' + img_fname + ' /></a>'
                elif field == 'status':
                    arg = formatter.get_status(doc[field])
                else:
                    arg = formatter.get_caption(doc[field])
                
                table += '<td>' + arg + '</td>'
                table += '</tr>'
                tr_is_open = False

            # Inner table with build results
            table += '<tr>'
            rowspan = len([ ts for ts in conf['tasks'] if 'result' in tasks[ts['name']]])
            table += '<th rowspan=' + str(rowspan) + ' >Results</th>'

            tr_is_open = True
            for task_spec in conf['tasks']:

                if 'result' not in tasks[task_spec['name']]:
                    continue
                
                if not tr_is_open:
                    table += '<tr>'

                title = task_spec['title'] if 'title' in task_spec else task_spec['name']
                table += '<th>' + title + '</th>'

                arg = ''

                results = tasks[task_spec['name']]['result']
                for return_spec in task_spec['return']:

                    if 'visibility' in return_spec and return_spec['visibility'] == 'hidden':
                        continue

                    ret_name = return_spec['name']
                    if ret_name not in results:
                        continue

                    arg += '<table class = result>'

                    if return_spec['type'] == 'bool':
                        head_name = return_spec['title'] if 'title' in return_spec else ret_name
                        arg += '<tr><th>' + head_name + '</th></tr>'
                        arg += '<tr><td>' + str(results[ret_name]) + '</td></tr>'
                    elif return_spec['type'] == 'table':
                        res_table = results[ret_name]

                        arg += '<tr>' + ''.join(['<th>' + str(k) + '</th>' for k in res_table['head']]) + '</tr>'
                        for row in res_table['body']:
                            arg += '<tr>' + ''.join(['<td>' + str(v) + '</td>' for v in row]) + '</tr>'

                    arg += '</table>'

                if arg == '':
                    arg = '&lt;optimized out&gt;'

                table += '<td>' + arg + '</td>'
                table += '</tr>'
                tr_is_open = False

            # Kill link
            table += '<tr>'
            table += '<td/><td align="center" bgcolor="red"><b><font color="white" size+=1>'
            if doc['status'] in ['Failed', 'Done', 'Killing']:
                table += '<a href=' + doc['_id'] + '.rem>R E M O V E</a>'
            else:
                table += '<a href=' + doc['_id'] + '.kill>K I L L</a>'
            table += '</font></b></td><td/>'
            table += '<tr>'

            table += '</table><br/>'
        return table


    def __sort_task_configs(self, all_task_configs):
        return all_task_configs.values() # TODO add sorting based on refs


    def __construct_input_form(self):
        form = '<h2>MyForm</h2><form action=/ method=POST ENCTYPE=multipart/form-data>'

        # load task configs
        wd = os.path.dirname(os.path.realpath(__file__))
        tasks_dir = os.path.join(wd, 'tasks')
        logger.debug('Looking up for task modules in ' + tasks_dir)
        all_task_configs = dict()
        for task_dir in os.listdir(tasks_dir):
            task_dir = os.path.join(tasks_dir, task_dir)
            logger.debug('pending {0}'.format(task_dir))
            if not os.path.isdir(task_dir):
                continue

            logger.debug('loading task from ' + task_dir)

            task_config_fname = os.path.join(task_dir, 'config.json')
            if not os.path.isfile(task_config_fname):
                logger.warning('skipping task {0} becase config.json is missed'.format(task_dir))
                continue

            with open(task_config_fname) as f:
                task_config = json.load(f)
                logger.debug('task {0} config is loaded: {1}'.format(task_dir, task_config))

            all_task_configs[task_config['name']] = task_config

        # construct tasks form
        sorted_tasks = self.__sort_task_configs(all_task_configs)
        for task in sorted_tasks:
            title = task['title'] if 'title' in task else task['name']
            form += '<fieldset><legend>' + title + '</legend>'

            for param in task['args']: #  {"name": "some_field", "type": "text", "title": "User Name"}
                if 'ref' in param: # skip referenced params
                    continue

                name = param['name']
                key = task['name'] + '.' + name
                tkey = task['name'] + '.' + name + '.type'
                title = param.get('title', name)
                input_type = param['type'] if param['type'] != 'bool' else 'checkbox'
                required = 'required' if 'required' in param and param['required'] else ''

                form += '<fieldset><legend>{0}</legend>'.format(title)

                if param['type'] in ['bool', 'text', 'file']:
                    form += '<input type="{0}" name="{1}" "{2}" />'.format(input_type, key, required)
                    form += '<input type="hidden" name="{0}" value="{1}" />'.format(tkey, param['type'])

                elif param['type'] in ['radio', 'checkbox']:
                    if 'values' not in param:
                        logger.warning('values are missed for param {0}'.format(name))
                    else:
                        for value in param['values']:
                            form += '<input type={0} name={1} value={2} {3} /> {4} <br/>'.format(input_type, key, value, required, value)
                        form += '<input type="hidden" name="{0}" value="{1}" />'.format(tkey, param['type'])
                else:
                    logger.warning('incorrect param {0} type {1}'.format(name, param['type']))
                
                form += '</fieldset>'

            form += '</fieldset>'

        form += '<input type=submit value="Upload patch">'
        form += '</form>'
        return form


    def get_message(self, db_answer=None):
        return message


    def under_construction(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        buf = '<html><meta charset="UTF-8"><body><h1>UNDER CONSTRUCTION</h1></body></html>'
        self.wfile.write(buf)


    def __get_form(self):
        form = '<html><head><title>DTS</title><meta charset="UTF-8">\
                <link rel = "stylesheet" type = "text/css" href = "style.css">\
                </head><body><table><tr><td style="padding:10px;vertical-align:top;">'

        form += self.__construct_input_form()

        form += '</td><td><div class=form>'

        #form += self.__construct_result_table(conf)

        form += '</div></td></tr></table>'
        form += '</body></html>'

        self.send_response(200)
        self.end_headers()
        self.wfile.write(form.encode('utf-8'))


    def do_GET(self):
        if self.path.endswith('.log'):
            self.__get_log()
        else:
            self.__get_form()


    def do_POST(self):
        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                environ={'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': self.headers['Content-Type'], })
        tasks_args = {}

        for k in form:
            if k.endswith('.type'):
                tkey_list = k.split('.')
                task_name = tkey_list[0]
                param_name = tkey_list[1]
                param_type = form[k].value

                logger.debug('checking field {0}'.format(k))
                
                k_value = k.replace('.type', '')
                param_val = form[k_value] if k_value in form else None

                logger.debug('field {0} values are {1}'.format(k_value, 'found' if param_val is not None else 'not-found'))
                logger.debug('field {0}:\n{1}'.format(k_value, param_val))

                task_args = tasks_args.setdefault(task_name, dict()).setdefault('args', dict())

                if param_type == 'bool':
                    task_args[param_name] = param_val is not None
                elif param_type in ['text', 'radio']:
                    task_args[param_name] = None if param_val is None or param_val.value == '' else param_val.value
                elif param_type == 'checkbox':
                    task_args[param_name] = list()
                    if param_val is not None:
                        if isinstance(param_val, list):
                            for v in param_val:
                                task_args[param_name].append(v.value)
                        else:
                            task_args[param_name].append(param_val.value)
                    else:
                        task_args[param_name] = None
                elif param_type == 'file':
                    if param_val is None or param_val.filename == '':
                        task_args[param_name] = None
                    else:
                        filename = os.path.basename(param_val.filename)
                        buf = param_val.file.read()
                        task_args[param_name] = {'name': filename, 'content': buf}
                else:
                    logger.warning('incorrect param type {0} in request'.format(param_type))

        print tasks_args


        self.send_response(200)
        self.end_headers()

        self.wfile.write('<html><head><title>Upload</title></head><body>\
                         Success\
                         <FORM><INPUT Type="button" VALUE="Back" onClick="history.go(-1);return true;"></FORM>\
                         </body></html>')

        doc = {'version': 1,
               'timestampt': time.time(),
               'host': None,
               'tasks': tasks_args}

        couch = couchdb.Server('http://{0}', srv_addr)
        db = couch['requests']
        db.save(doc)


class ForkingHTTPServer(SocketServer.ForkingMixIn, BaseHTTPServer.HTTPServer):
    def finish_request(self, request, client_address):
        request.settimeout(30)
        # "super" can not be used because BaseServer is not created from object
        BaseHTTPServer.HTTPServer.finish_request(self, request, client_address)


if __name__ == '__main__':
    logger.basicConfig(level=logger.DEBUG)
    handler_class = MyHandler
    try:
        port = 8080 
        url = "http://bop:%d/" % port
        print "Ask user to visit this URL:\n\t%s" % url
        srvr = ForkingHTTPServer(('', port), handler_class)
        srvr.serve_forever()  # serve_forever
    except KeyboardInterrupt:
        pass
