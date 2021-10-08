from hai.pipe_pump import CRLFPipePump


class CrlfTestHandler:
    def __init__(self):
        self.log = []
        self.lines = []
        self.raw_lines = []

    def handle_crlf_input(self, key, old_content, new_content, is_replace):
        if is_replace:
            if new_content:
                self.log.append(f'Replace {old_content} with {new_content}')
                self.lines[-1] = new_content
                self.raw_lines.append(new_content)
        else:
            self.log.append(f'Print {new_content}')
            self.lines.append(new_content)
            self.raw_lines.append(new_content)


def do_crlf_test(input):
    handler = CrlfTestHandler()
    cpp = CRLFPipePump()
    cpp.add_handler(handler.handle_crlf_input)
    cpp.register('test', None)
    cpp.feed('test', input)
    cpp.close()
    return handler


def test_crlf_pipe_pump():
    input = b'''first\rreplaced first\nsecond\r\rreplaced second\n\r\r\rthird\n\n\nfourth'''
    handler = do_crlf_test(input)
    assert handler.lines == [b'replaced first', b'replaced second', b'third', b'', b'', b'fourth']
    assert handler.raw_lines == [
        b'first',
        b'replaced first',
        b'second',
        b'replaced second',
        b'',
        b'third',
        b'',
        b'',
        b'fourth',
    ]


def test_crlf_pipe_pump_rn():
    handler = do_crlf_test(b'''oispa\r\nkaljaa''')
    assert handler.lines == handler.raw_lines == [b'oispa', b'kaljaa']
