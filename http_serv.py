#! /usr/bin/env python
# coding=utf-8

import os, cgi, re
import couchdb
import BaseHTTPServer


class GetHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    hconf = {
            'user'     : {'title': 'User', 'type':'text'},
            'filename' : {'title': 'Patch', 'type': 'file'},
            'branch'   : {'title': 'Branch', 'type': 'radio', 'values': ['10_0', '10_1', 'trunk']},
            'defects'  : {'title': 'Enable defects?', 'type': 'checkbox', 'values': ['yes']},
            'buildspec': {'title': 'Create buildspec?', 'type': 'checkbox', 'values': ['yes']},
            'systems'  : {'title': 'Test systems', 'type': 'checkbox', 'values': ['EngineTest', 'T_and_V']},
            'projects' : {'title': 'Projects', 'type': 'checkbox',
                         'values': ['android-5.0.2_r1', 'boost_1_57_0', 'firefox-35', 'linux-3.18.1']}
            }

    def get_log(self):
        f = open(os.curdir + os.sep + self.path)
        self.send_response(200)
        self.send_header('Content-type',    'text/html')
        self.end_headers()
        buf = '<html><body><code>'
        buf += f.read()
        buf = buf.replace('\n', '<br/>') + '</code></body></html>'
        self.wfile.write(buf)
        f.close()

    def get_kill(self):
        couch = couchdb.Server()
        db = couch['patches']
        idx = re.search('/(.+?)\.kill', self.path).group(1)
        if idx not in db:
            return
        doc = db[idx]
        doc['status'] = 'Killing'
        db.save(doc)
        self.send_response(200)
        self.send_header('Content-type',    'text/html')
        self.end_headers()
        buf = '<html><body><h1>Killing '+ doc['filename'] +'</h1></body></html>'
        self.wfile.write(buf)


    def construct_result_table(self):
        table = '<table border=1"><tr>'

        couch = couchdb.Server()
        db = couch['patches']
        docs = [ db[idx] for idx in db ]

        # head
        table += '<tr>'
        for k, v in sorted(self.hconf.items(), key=lambda x: x[1]):
            table += '<th>' + v['title'] + '</th>'
        table += '<th>' + 'Status' + '</th>'
        table += '</tr>'

        # cells
        for doc in reversed(docs):
            table += '<tr>'
            for k, v in sorted(self.hconf.items(), key=lambda x: x[1]):
                table += '<td>' + str(doc[k]) + '</td>'
            if 'status' in doc:
                table += '<td>' + str(doc['status']) + '</td>'
            table += '</tr>'
            # inner table
            if 'res' in doc:
                table += '<tr>'
                table += '<td colspan=' + str(len(self.hconf.keys()) + 1) + ' >'
                for k,v in doc['res'].items(): # k = project, v = { android : { attr : val, ... }, ... }
                    table += '<table border=1 style="padding:10px;">'

                    attrs = v.items() # [ (android, {attr1: val, ... }), ... ]

                    table += '<tr><th>' + k + '</th>'
                    for attr_name, attr_val in sorted(attrs[0][1].items(), key=lambda x: x[0]):
                        table += '<th>' + attr_name + '</th>'
                    table += '</tr>'

                    for proj_name, proj_attrs in attrs:
                        table += '<tr><td>' + proj_name + '</td>'
                        for attr_name, attr_val in sorted(proj_attrs.items(), key=lambda x: x[0]):
                            table += '<td>' + str(attr_val) + '</td>'
                        table += '</tr>'
                    table += '</table>'
                table += '</tr>'
        table += '</table>'
        return table


    def construct_input_form(self):
        input_form = '<h2>Send patch to testing machine</h2>'
        input_form += '<form action=/ method=POST ENCTYPE=multipart/form-data>'

        for k,v in sorted(self.hconf.items(), key=lambda x: x[1]):
            input_form += '<fieldset><legend>' + v['title'] + '</legend>'

            if 'values' in v:
                for val in v['values']:
                    name = k if v['type'] == 'radio' else k + val
                    value = val if v['type'] == 'radio' else 'enabled'

                    input_form += '<input type=' + v['type'] + ' name=' + name + ' value=' + value + ' />' + val + '<br/>'
            else:
                input_form += '<input type=' + v['type'] + ' name=' + k + ' />'

            input_form += '</fieldset>'

        input_form += '<p><input type=submit value=Upload></p>'
        input_form += '</form>'
        return input_form


    def get_default(self):
        result_table = self.construct_result_table()
        input_form = self.construct_input_form()

        message = '<html><head><title>Upload</title></head><body>\
                     <table><tr><td style="padding:10px;vertical-align:top;">'
        message += input_form
        message += '</td><td style="padding:10px;vertical-align:top;">'
        message += result_table
        message += '</td></tr></table></body></html>'

        self.send_response(200)
        self.end_headers()
        self.wfile.write(message)


    def do_GET(self):
        if self.path.endswith(".log") or self.path.endswith("realtime"):
            self.get_log()
        if self.path.endswith(".kill?") or self.path.endswith(".kill"):
            self.get_kill()
        else:
            self.get_default()


    def do_POST(self):
        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                environ={'REQUEST_METHOD':'POST', 'CONTENT_TYPE':self.headers['Content-Type'], })
        doc = {}

        for k,v in self.hconf.items():
            doc[k] = None

            if v['type'] == 'radio' and k in form:
                doc[k] = form[k].value

            elif v['type'] == 'checkbox':
                doc[k] = []
                for val in v['values']:
                    if k+val in form:
                        doc[k].append(val)

            elif v['type'] == 'text' and k in form:
                doc[k] = form[k].value

            elif v['type'] == 'file' and k in form:
                upfile = form[k]
                if '\\' in upfile.filename:
                    filename = upfile.filename.split('\\')[-1]
                else:
                    filename = upfile.filename.split('/')[-1]
                filename = re.sub('[ \t]','-', filename)
                filename = re.sub('[^a-zA-Z0-9_.:-]','', filename)

                fp = open(filename, 'wb')
                while True:
                    chunk = upfile.file.read(8192)
                    if len(chunk) == 0:
                        break
                    else:
                        fp.write(chunk)
                fp.close()
                doc[k] = filename

        self.send_response(200)
        self.end_headers()

        self.wfile.write('<html><head><title>Upload</title></head><body>' + str(doc)+ '</body></html>')

        couch = couchdb.Server()
        db = couch['patches']
        db.save(doc)


if __name__ == '__main__':
    try:
        port = 8080 #random.randint(50000,60000)
        url = "http://adumu:%d/" % (port)
        server = BaseHTTPServer.HTTPServer( ('', port), GetHandler )
        print "Ask user to visit this URL:\n\t%s" % url
        server.serve_forever()
    except KeyboardInterrupt:
        pass

