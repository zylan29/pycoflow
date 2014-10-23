import os


def list_files(a_dir, postfix=''):
    root, _, files = os.walk(a_dir).next()
    if not root.endswith("/"):
        root += "/"
    if postfix == '':
        return map(lambda f: root + f, files)
    else:
        return map(lambda f: root + f, filter(lambda a_file: a_file.endswith(postfix), files))


def filename_to_hostname(file_path):
    """
    :param file_path:  format is hostname.postfix
    :return: hostname
    """
    _, file_name = os.path.split(file_path)
    return file_name.split(".")[0]