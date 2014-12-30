import re

dictionary = {
    '10_0': '10.0',
    '10_1': '10.1',
    'trunk': '<span class = tag>trunk</span>',
    'EngineTests': 'Run Engine Tests',
    'TaV': 'Run T&V',
    'defects': 'Diff for defects',
    'buildspec': 'Re-create buildspec',
    'android-5.0.2_r1': 'Android <span class = tag>5.0.2&nbsp;R1</span>',
    'boost_1_57_0': 'Boost <span class = tag>1.57.0</span>',
    'firefox-35': 'Firefox <span class = tag>35</span>',
    'linux-3.18.1': 'Linux <span class = tag>3.18.1</span>'
}


def get_caption(text):
    matcher = re.match('(?P<name>.*)\.patch', text)
    if matcher:
        return matcher.group('name') + '<span class = grey>.patch</span>'
    if text in dictionary:
        return dictionary[text]
    else:
        return text


def format_tag(text):
    """
    Format string for HTML representation.

    :param text: input string
    :return formatted string
    """
    if '#' in text:
        text = text[:text.find('#')] + ' <span class = tag>' + text[text.find('#') + 1:] + '</span>'
    return text.strip()


