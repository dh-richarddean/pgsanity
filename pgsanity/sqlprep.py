import re
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

CLEANUP_PATTERNS ={
    'distkey': (r'(DISTKEY|((COMPOUND|INTERLEAVED)\s*)?SORTKEY)\s*\(.*\)', re.MULTILINE | re.IGNORECASE, ''),
    'analyze': (r'PREDICATE\sCOLUMNS', re.IGNORECASE, ''),
    'diststyle': (r'DISTSTYLE\s\w*', re.MULTILINE | re.IGNORECASE, ''),
    'identity': (r'IDENTITY\s?\([0-9]*\,[0-9]*\s?\)', re.IGNORECASE, ''),
    'window_ignore_nulls': (r'ignore\s*nulls\s*\)', re.IGNORECASE, ')')
 }

def _cleanup(input_str):
    for pattern in CLEANUP_PATTERNS:
        regex, regex_opts, sub_with = CLEANUP_PATTERNS[pattern]
        regex_c = re.compile(regex, regex_opts)
        input_str = re.sub(regex_c, sub_with, input_str)
    return input_str

def prepare_sql(sql, add_semicolon=False):
    results = StringIO()

    in_statement = False
    in_line_comment = False
    in_block_comment = False
    for (start, end, contents) in split_sql(sql):
        precontents = None
        start_str = None

        # Run our cleaning process
        contents = _cleanup(contents)

        # decide where we are
        if not in_statement and not in_line_comment and not in_block_comment:
            # not currently in any block
            if start != "--" and start != "/*" and len(contents.strip()) > 0:
                # not starting a comment and there is contents
                in_statement = True
                precontents = "EXEC SQL "

        if start == "/*":
            in_block_comment = True
        elif start == "--" and not in_block_comment:
            in_line_comment = True
            if not in_statement:
                start_str = "//"

        start_str = start_str or start or ""
        precontents = precontents or ""
        if not (start_str + precontents + contents).isspace():
            results.write(start_str + precontents + contents)

        if not in_line_comment and not in_block_comment and in_statement and end == ";":
            in_statement = False

        if in_block_comment and end == "*/":
            in_block_comment = False

        if in_line_comment and end == "\n":
            in_line_comment = False

    response = results.getvalue()
    results.close()
    if add_semicolon and in_statement and not in_block_comment:
        if in_line_comment:
            response = response + "\n"
        response = response + ';'
    return response

def split_sql(sql):
    """generate hunks of SQL that are between the bookends
       return: tuple of beginning bookend, closing bookend, and contents
         note: beginning & end of string are returned as None"""
    bookends = ("\n", ";", "--", "/*", "*/")
    last_bookend_found = None
    start = 0

    while start <= len(sql):
        results = get_next_occurence(sql, start, bookends)
        if results is None:
            yield (last_bookend_found, None, sql[start:])
            start = len(sql) + 1
        else:
            (end, bookend) = results
            yield (last_bookend_found, bookend, sql[start:end])
            start = end + len(bookend)
            last_bookend_found = bookend

def get_next_occurence(haystack, offset, needles):
    """find next occurence of one of the needles in the haystack
       return: tuple of (index, needle found)
           or: None if no needle was found"""
    # make map of first char to full needle (only works if all needles
    # have different first characters)
    firstcharmap = dict([(n[0], n) for n in needles])
    firstchars = firstcharmap.keys()
    while offset < len(haystack):
        if haystack[offset] in firstchars:
            possible_needle = firstcharmap[haystack[offset]]
            if haystack[offset:offset + len(possible_needle)] == possible_needle:
                return (offset, possible_needle)
        offset += 1
    return None
