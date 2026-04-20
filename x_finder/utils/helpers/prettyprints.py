from pathlib import Path


BASE_DIR = str(Path(__file__).resolve().parent.parent)
BASE_URL = " http://127.0.0.1:8000/"


class Colors:
    OK = '\033[0;32m'
    KO = '\033[6;31m'
    END = '\033[0m'
    HEADER = '\033[4;33m'
    METHOD = '\033[1;33m'
    URL = '\033[4;34m'
    UPDATED = '\033[0;33m'

    @staticmethod
    def get(code, string):
        return getattr(Colors, code) + string + Colors.END


class PRR:
    BASE_URL = BASE_URL

    @staticmethod
    def _p(string, check,  p='s'):
        if isinstance(string, str) and check:
            return string + p
        return string

    @staticmethod
    def _o(string, length=2):
        string = str(string)
        while len(string) < length:
            string = '0' + string
        return string

    @staticmethod
    def colorize(string, check):
        if check:
            string = Colors.get('OK', string)
        else:
            string = Colors.get('KO', string)
        return string

    @staticmethod
    def format_list(list_of_dicts):
        if list_of_dicts:
            return [[PRR.format(k, v) for k, v in data.items()]
                    for data in list_of_dicts
                    ]
        return [[]]

    @staticmethod
    def format(key, value):
        if not key or not value:
            return key, str(value)
        if "date_" in key or "_date" in key or key == "payment_due":
            value = PRR.format_datetime(value)
        elif isinstance(value, dict):
            value = "{...}"
        elif isinstance(value, list):
            value = "[...]"
        else:
            value = str(value)
        if "contact" in key:
            value = value.split(':')[-1].strip()
        if key and len(key) > 20:
            key = key[:18] + '...'
        if value and len(value) > 23:
            value = value[:21] + '...'
        return key, value

    @staticmethod
    def format_datetime(dt):
        dt = str(dt)
        split = ' '
        if 'T' in dt:
            split = 'T'
        dts = dt.split(split)
        if len(dts) == 1:
            return dt + ' 00:00'
        if len(dts[1]) > 5:
            return dt.split(split)[0] + ' ' + dt.split(split)[1][:5]
        return dt.split(split)[0] + ' ' + dt.split(split)[1]

    @staticmethod
    def compare_datetimes(dt_1, dt_2):
        if dt_1 == dt_2:
            return True
        try:
            trouble = ["23:59", "00:00"]
            check_1 = abs(int(dt_1[-2:]) - int(dt_2[-2:])) % 60 < 5
            check_2 = abs(int(dt_1[-5:-3]) - int(dt_2[-5:-3])) % 24 < 2
            check_3 = dt_1[:10] == dt_1[:10] or dt_1[-5:] in trouble
            return check_1 and check_2 and check_3
        except IndexError:
            return False

    @staticmethod
    def prettify_key_value(key, value, offset=0, checks=(None, None)):
        if key:
            len_key = len(key)
            len_value = len(value)
            if checks[0] is not None:
                key = PRR.colorize(key, checks[0])
            if checks[1] is not None:
                value = PRR.colorize(value, checks[1])
            result = (23 - offset) * ' ' + key + (offset - len_key) * ' '
            result += ' : ' + value + (23 - len_value) * ' '
        else:
            result = 49 * ' '
        return result

    @staticmethod
    def get_longest_key(list_of_key_values):
        longest = 0
        for key_value in list_of_key_values:
            longest = max(longest, len(key_value[0]))
        return longest

    @staticmethod
    def get_headers(method, display_expected):
        headers = 17 * ' '
        expect = 'Expected Result'
        if method == 'PUT':
            headers += '  '
            expect = 'Target Item'
        if method in ['POST', 'PUT']:
            headers += Colors.HEADER + 'Request  Body' + Colors.END + 35 * ' '
        headers += Colors.HEADER + 'Response Data' + Colors.END + 35 * ' '
        if display_expected:
            headers += Colors.HEADER + expect + Colors.END
        return headers

    @staticmethod
    def get_errors(k_mismatch, v_mismatch, updated=0):
        string_1 = f"{k_mismatch} key error{'s' if k_mismatch > 1 else ''}. "
        total = PRR.colorize(string_1, k_mismatch == 0)
        string_2 = f"{v_mismatch} value error{'s' if v_mismatch > 1 else ''}. "
        total += PRR.colorize(string_2, v_mismatch == 0)
        string_3 = f"{updated} value{'s' if updated > 1 else ''} updated."
        if updated > 0:
            string_3 = Colors.get('UPDATED', string_3)
            total += string_3
        return total

    @staticmethod
    def get_title(method, url, logs=None):
        title = 'Request:  ' + Colors.get('METHOD', method) + '   '
        title += Colors.get('URL', PRR.BASE_URL + url)
        title += ' ' * (100 - len(method) - len(PRR.BASE_URL + url))
        if logs and "email" in logs.keys() and "password" in logs.keys():
            email = PRR.colorize("email", False) + '=' + logs["email"]
            password = PRR.colorize("password", False) + '=' + logs["password"]
            title += f'login: {email} {password}'
        else:
            title += PRR.colorize('login required', False)
        return title

    @staticmethod
    def save_report(report_rows, action, model='', app='crm', mode='a'):
        if model:
            model += '\\'
        file = BASE_DIR + f'\\doc\\{app}\\{model}{action}.txt'
        with open(file, mode, encoding='cp1252') as f:
            for row in report_rows:
                print(row, file=f)

    @staticmethod
    def print_doc(action, model='', app='crm'):
        if model:
            model += '\\'
        file = BASE_DIR + f'\\doc\\{app}\\{model}{action}.txt'
        with open(file, 'r', encoding='cp1252') as f:
            for row in f:
                print(row, end='')


class Report(PRR):
    """A little helper to format the kwargs passed to the PRR subclass
    kwargs: url(string, action(string)), logs(dict), request_body(dict),
            expected(dict), response_body(dict), display_expected(bool),
            display_errors(bool), mapping(tuple of positive int)
    Dicts are turned in lists of dicts.
    """
    key_errors = 0
    value_errors = 0
    updated = 0

    def __init__(self, url="",
                 action="",
                 logs=None,
                 request_body=None,
                 response_body=None,
                 expected=None,
                 display_expected=False,
                 display_errors=False,
                 mapping=()):

        self.url = url
        if logs is None:
            logs = {}
        self.logs = logs
        self.action = action
        if self.action.lower() in ['list', 'detail', 'search']:
            self.method = 'GET'
        elif self.action.lower() == 'add':
            self.method = 'POST'
        elif self.action.lower() == 'change':
            self.method = 'PUT'
        else:
            self.method = action.upper()
        if request_body is None:
            self.request_body = None
        else:
            self.request_body = self.format_list([request_body])
        if expected is None:
            self.expected = None
        else:
            if isinstance(expected, dict):
                expected = [expected]
            self.expected = self.format_list(expected)
        if response_body is None:
            self.response_body = None
        else:
            if isinstance(response_body, dict):
                response_body = [response_body]
            self.response_body = self.format_list(response_body)
        self.display_errors = display_errors
        self.mapping = sorted(list(mapping), reverse=True)
        if self.expected:
            self.initial_data = self.expected[0]
        else:
            self.initial_data = []
        if self.response_body:
            self.longest_key = self.get_longest_key(self.response_body[0])
        if self.request_body:
            longest_key = self.get_longest_key(self.request_body[0])
            self.longest_key = max(self.longest_key, longest_key)
        self.display_expected = False
        if len(self.initial_data) > 0 and display_expected:
            self.display_expected = True
        self.display_errors = display_errors

        self.title = self.get_title(self.method,
                                    self.url,
                                    self.logs)
        self.result = self.get_pretty_rows()
        self.sum_events = self.key_errors + self.value_errors + self.updated
        check = self.display_expected or self.sum_events != 0
        self.headers = self.get_headers(self.method, check)
        self.errors = self.get_errors(self.key_errors,
                                      self.value_errors,
                                      self.updated)
        self.report = [self.title]
        self.report += [65 * ' ' + 10 * '-', self.headers] + self.result
        if self.display_errors:
            self.report.append(self.errors + '\n')

    def get_pretty_rows(self):
        if self.method == 'POST':
            return self.get_pretty_post_rows()
        if self.method == 'PUT':
            return self.get_pretty_put_rows()
        if self.method == 'GET':
            return self.get_pretty_get_rows()
        return []

    def print(self):
        for row in self.report:
            print(row)

    def save(self, model='', app='crm', mode='a'):
        super().save_report(self.report, self.action, model, app, mode)

    def get_pretty_post_rows(self):
        """
        Builds each row with 2 (resp. 3) key_value pairs according to
        display_expected option.
        Checks each key and each value with expected result if provided.
        Failed checks are colored in red, successful ones in green.
        Returns the list of colored and justified rows.
        """
        result = []
        if self.request_body:
            request_body = self.request_body[0]
        else:
            request_body = []
        if self.response_body:
            response_body = self.response_body[0]
        else:
            response_body = []
        expected = self.initial_data
        for index in self.mapping:
            if index < len(request_body):
                req = request_body
                request_body = req[:index] + [("", "")] + req[index:]

        while len(request_body) < max(len(expected), len(response_body)):
            request_body += [("", "")]
        while len(response_body) < len(expected):
            response_body += [("", "")]

        if len(expected) > 0:
            raw_table = zip(request_body,
                            expected,
                            response_body)
            for body_row, expected_row, response_row in raw_table:
                row = self.prettify_key_value(*body_row, self.longest_key)

                checks = (expected_row[0] == response_row[0],
                          expected_row[1] == response_row[1])
                if checks[0] and response_row[0] == 'date_created':
                    checks = (checks[0],
                              PRR.compare_datetimes(expected_row[1],
                                                    response_row[1])
                              )
                row += self.prettify_key_value(*response_row,
                                               self.longest_key,
                                               checks)
                if not checks[0] or not checks[1]:
                    if not checks[0]:
                        self.key_errors += 1
                    if not checks[1]:
                        self.value_errors += 1
                    row += self.prettify_key_value(*expected_row,
                                                   self.longest_key,
                                                   checks)
                elif self.display_expected:
                    row += self.prettify_key_value(*expected_row,
                                                   self.longest_key,
                                                   checks)
                result.append(row)
        else:
            for body_row, response_row in zip(request_body,
                                              response_body):
                row = self.prettify_key_value(*body_row, self.longest_key)
                row += self.prettify_key_value(*response_row,
                                               self.longest_key)
                result.append(row)
        result.append(65 * ' ' + 10 * '-')
        return result

    def get_pretty_put_rows(self):
        """
        Builds each row with 2 (resp. 3) key_value pairs according to
        display_expected option as cells.
        Checks each key and each value with expected result if provided.
        Failed checks are colored in red, successful ones in green.
        Returns the list of colored and justified rows.
        """
        result = []
        if self.request_body:
            request = dict(self.request_body[0])
        else:
            return self.get_pretty_get_rows()
        initial = dict()
        if self.initial_data:
            initial = dict(self.initial_data)
        if self.response_body:
            response = dict(self.response_body[0])
        else:
            response = dict()
        if len(self.initial_data) == 0:
            initial = response
        else:
            for key, value in response.items():
                row = ''
                if key in request.keys() and key in initial.keys():
                    check = request[key] == response[key]
                    updated = check and initial[key] != response[key]
                    row += self.prettify_key_value(key,
                                                   request[key],
                                                   self.longest_key,
                                                   (True, check))
                    row += self.prettify_key_value(key,
                                                   value,
                                                   self.longest_key,
                                                   (True, check))
                    if self.display_expected or updated or not check:
                        expected = self.prettify_key_value(key,
                                                           initial[key],
                                                           self.longest_key)
                        if updated:
                            expected = Colors.get('UPDATED', expected)
                            self.updated += 1
                        row += expected
                    if not check:
                        self.value_errors += 1
                    request.pop(key, None)
                    initial.pop(key, None)
                elif key in initial.keys():
                    row += ' ' * 49
                    check = initial[key] == value or key == "date_updated"
                    row += self.prettify_key_value(key,
                                                   value,
                                                   self.longest_key,
                                                   (True, check))
                    if self.display_expected:
                        row += self.prettify_key_value(key,
                                                       initial[key],
                                                       self.longest_key,
                                                       (True, check))
                    if not check:
                        self.value_errors += 1
                    initial.pop(key, None)
                elif key in request.keys():
                    row += self.prettify_key_value(key,
                                                   request[key],
                                                   self.longest_key,
                                                   (False, None))
                    row += self.prettify_key_value(key,
                                                   value,
                                                   self.longest_key,
                                                   (False, None))
                    self.key_errors += 1
                    request.pop(key, None)
                else:
                    row += ' ' * 49
                    if key != "date_updated":
                        row += self.prettify_key_value(key,
                                                       value,
                                                       self.longest_key,
                                                       (False, None))
                        self.key_errors += 1
                    else:
                        pretty = self.prettify_key_value(key,
                                                         value,
                                                         self.longest_key)
                        row += Colors.get('UPDATED', pretty)
                result.append(row)
        for key, value in initial.items():
            row = ''
            if key in request.keys():
                check = request[key] == value
                row += self.prettify_key_value(key,
                                               request[key],
                                               self.longest_key,
                                               (True, check))
                if self.display_expected or not check:
                    row += ' ' * 49
                    row += self.prettify_key_value(key,
                                                   value,
                                                   self.longest_key,
                                                   (True, check))
                if not check:
                    self.value_errors += 1
                request.pop(key, None)
            else:
                row += ' ' * 49
                row += self.prettify_key_value(key,
                                               value,
                                               self.longest_key,
                                               (False, None))
                self.key_errors += 1
            result.append(row)

        for key, value in request.items():
            result.append(self.prettify_key_value(key,
                                                  value,
                                                  self.longest_key,
                                                  (False, False)))
        result.append(65 * ' ' + 10 * '-')
        return result

    def get_pretty_get_rows(self):
        result = []
        if not self.expected and self.response_body:
            for resp in self.response_body:
                for key, value in resp:
                    result.append(self.prettify_key_value(key,
                                                          value,
                                                          self.longest_key))
                result.append(65 * ' ' + 10 * '-')
            return result
        if not self.response_body:
            return result

        expects = [dict(exp) for exp in self.expected]
        responses = [dict(resp) for resp in self.response_body]
        for resp, expect in zip(responses, expects):
            for key, value in resp.items():
                check_1 = key in expect.keys()
                check_2 = check_1 and expect[key] == value
                row = self.prettify_key_value(key,
                                              value,
                                              self.longest_key,
                                              (check_1, check_2))
                if check_1 and (self.display_expected or not check_2):
                    row += self.prettify_key_value(key,
                                                   expect[key],
                                                   self.longest_key,
                                                   (True, check_2))
                if not check_1:
                    self.key_errors += 1
                if not check_2:
                    self.value_errors += 1
                result.append(row)
            result.append(65 * ' ' + 10 * '-')
        return result
