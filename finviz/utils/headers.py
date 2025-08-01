from typing import List


def patch_dup_headers(headers: List[str]) -> List[str]:
    header_suffix = {}
    uniq_headers = []
    for header in headers:
        if header in header_suffix:
            header_suffix[header] += 1
            header = header + str(header_suffix[header])
        else:
            header_suffix[header] = 0
        uniq_headers.append(header)
    return uniq_headers
