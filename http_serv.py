#! /usr/bin/env python
# coding=utf-8

import BaseHTTPServer
import cgi
import couchdb
import formatter
import os
import re


class GetHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    hconf = {
        'user': {'title': 'User', 'type': 'text'},
        'filename': {'title': 'Patch<br />file', 'type': 'file', 'style': 'tt'},
        'branch': {'title': 'Branch', 'type': 'radio', 'values': ['10_0', '10_1', 'trunk']},
        'options': {'title': 'Options', 'type': 'checkbox', 'values': ['defects', 'buildspec']},
        'systems': {'title': 'Test<br />systems', 'type': 'checkbox', 'values': ['EngineTests', 'TaV']},
        'projects': {'title': 'Projects', 'type': 'checkbox',
                     'values': ['android-5.0.2_r1', 'boost_1_57_0', 'firefox-35', 'linux-3.18.1']}
        }

    def get_log(self):
        f = open(os.curdir + os.sep + self.path)
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        buf = '<html><body><code>'
        buf += f.read()
        buf = buf.replace('\n', '<br/>') + '</code></body></html>'
        self.wfile.write(buf)
        f.close()

    def get_css(self):
        f = open(os.curdir + os.sep + self.path)
        self.send_response(200)
        self.send_header('Content-type', 'text/css')
        self.end_headers()
        buf = f.read()
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
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        buf = '<html><body><h1>Killing ' + doc['filename'] + '</h1></body></html>'
        self.wfile.write(buf)

    def construct_result_table(self, db_answer=None):
        table = '<table class = main>'

        if not db_answer:
            couch = couchdb.Server()
            db = couch['patches']
            docs = [db[idx] for idx in db]
        else:
            docs = db_answer

        # head
        table += '<tr>'
        for k, v in sorted(self.hconf.items(), key=lambda x: x[1]):
            if k == 'filename':
                caption = 'Patch file and user'
            elif k in ['user']:  # Hidden fields in result table
                continue
            elif 'type' in v:
                caption = v['title']
            else:
                continue
            table += '<th>' + caption + '</th>'
        table += '<th>Realtime log</th>'
        table += '</tr>'

        index = 0
        display_items = 2

        # cells
        for doc in reversed(docs):
            table += '<tr>'
            for k, v in sorted(self.hconf.items(), key=lambda x: x[1]):
                td = ''
                if k == 'filename':
                    td += '<tt>' + formatter.get_caption(str(doc[k])) + '</tt><br /><span class = user>' + \
                          formatter.get_caption(str(doc['user'])) + '</span>'
                elif k == 'user':
                    continue
                elif 'type' in v:
                    if isinstance(doc[k], list):
                        for item in doc[k][:display_items]:
                            td += formatter.get_caption(item) + '<br />'
                        if len(doc[k]) > display_items:
                            index += 1
                            td += '<span id = "hidden' + str(index) + '" style = "display: none">'
                            for item in doc[k][display_items:]:
                                td += formatter.get_caption(item) + '<br />'
                            td += '</span><div class = "more" id = "more' + str(index) + \
                                  '" onclick = "myFunction(' + str(index) + ')">more</div>'
                    else:
                        td = formatter.get_caption(str(doc[k]))
                    if 'style' in v:
                        td = '<span class = ' + v['style'] + '>' + td + '</span>'
                else:
                    continue
                table += '<td>' + td + '</td>'
            table += '<td><a href=' + doc['_id'] + '/realtime >log</a></td>'
            if 'status' in doc:
                table += '<td>' + formatter.get_status(doc['status'][0]) + '</td>'
            table += '</tr>'
        table += '</table>'
        return table

    def construct_input_form(self):
        input_form = '<h2>Send patch to testing machine</h2>'
        input_form += '<div class = send><form action=/ method=POST ENCTYPE=multipart/form-data>'
        input_form += '<table class = parameters>'

        for k, v in sorted(self.hconf.items(), key=lambda x: x[1]):
            input_form += '<tr><td class = label>' + v['title'] + '</td><td>'

            if 'values' in v:
                for val in v['values']:
                    name = k if v['type'] == 'radio' else k + val
                    value = val if v['type'] == 'radio' else 'enabled'

                    caption = formatter.get_caption(val)

                    input_form += '<p class = ti><label class = ' + v['type'] + '><input type=' + v['type'] + \
                                  ' name=' + name + ' value=' + value + ' /><span class = branch>' + caption + \
                                  '</span></label></p>'
            else:
                input_form += '<input type=' + v['type'] + ' name=' + k + ' />'

            input_form += '</td></tr>'

        input_form += '<tr><td></td><td><input type=submit value="Upload patch"></td></tr>'
        input_form += '</table>'
        input_form += '</form></div>'
        return input_form

    def get_message(self, db_answer=None):
        result_table = self.construct_result_table(db_answer=db_answer)
        input_form = self.construct_input_form()

        message = '<html><head><title>Upload</title><link rel = "stylesheet" type = "text/css" href = "style.css">\
                     </head><body><table><tr><td style="padding:10px;vertical-align:top;">'
        message += input_form
        message += '</td><td><div class = table>'
        message += result_table
        message += '</div></td></tr></table>'
        message += '''<script>
function myFunction(number) {
    var text = document.getElementById("hidden" + number);
    var more = document.getElementById("more" + number);
    if (text.style.display == "none")
    {
        text.style.display = "block";
        more.innerHTML = "hide";
    }
    else
    {
        text.style.display = "none";
        more.innerHTML = "more";
    }
}
</script>
'''
        message += '</body></html>'
        return message

    def get_default(self):
        message = self.get_message()

        self.send_response(200)
        self.end_headers()
        self.wfile.write(message)

    def do_GET(self):
        if self.path.endswith(".log") or self.path.endswith("realtime"):
            self.get_log()
        elif self.path.endswith(".kill?") or self.path.endswith(".kill"):
            self.get_kill()
        elif self.path.endswith("css"):
            self.get_css()
        else:
            self.get_default()

    def do_POST(self):
        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                environ={'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': self.headers['Content-Type'], })
        doc = {}

        for k, v in self.hconf.items():
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
                filename = re.sub('[ \t]', '-', filename)
                filename = re.sub('[^a-zA-Z0-9_.:-]', '', filename)

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

        self.wfile.write('<html><head><title>Upload</title></head><body>' + str(doc) + '</body></html>')

        couch = couchdb.Server()
        db = couch['patches']
        db.save(doc)

    def test(self, docs):
        return self.get_message(db_answer=docs)

if __name__ == '__main__':
    try:
        port = 8080  # random.randint(50000,60000)
        url = "http://adumu:%d/" % port
        server = BaseHTTPServer.HTTPServer(('', port), GetHandler)
        print "Ask user to visit this URL:\n\t%s" % url
        server.serve_forever()
    except KeyboardInterrupt:
        pass
