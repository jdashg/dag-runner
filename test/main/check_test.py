assert __name__ == '__main__'

import pathlib

serial_txt = pathlib.Path('../out/serial.txt').read_text().strip()
assert serial_txt == '3', serial_txt

parallel_txt = pathlib.Path('../out/parallel.txt').read_text().strip()
parallel_lines = [x.strip() for x in parallel_txt.split('\n')]
assert parallel_lines == ['1', '1', '1'], parallel_lines
