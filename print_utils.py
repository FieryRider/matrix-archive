import sys

class PrintUtils:
    progress = 0
    total_progress = 0

    @classmethod
    def print_progress_bar(cls, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', print_end = "\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            print_end    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        cls.progress = iteration
        cls.total_progress = total

        if prefix != "":
            prefix += " "
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'{prefix}[{bar}] {percent}% {suffix}', end = print_end)
        if iteration == total:
            print()

    @classmethod
    def smart_print(cls, msg, end = "\x1b[2K\r\n", file = sys.stdout):
        if not isinstance(msg, str):
            msg = str(msg)

        print("\x1b[2K", end = "\r", flush = True)
        if file == sys.stderr:
            print("\x1b[31m" + msg + "\x1b[37m", file = sys.stderr, flush = True)
        else:
            print(msg, flush = True)
        if cls.total_progress != 0:
            cls.print_progress_bar(cls.progress, cls.total_progress)
