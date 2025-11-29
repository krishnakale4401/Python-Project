
import re

def parse_file_date_from_name(fname):


    s = fname
    date = re.findall(r'\d+', s)[0]

    return date