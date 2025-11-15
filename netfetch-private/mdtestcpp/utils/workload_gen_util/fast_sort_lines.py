# WORKLOAD_HOME_DIR = '/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/workload'

def sort_lines(input_file,output_file):
    # input_file = f"{WORKLOAD_HOME_DIR}/file.out.fast"
    # output_file = f"{WORKLOAD_HOME_DIR}/file2.out"

    # Read, process, and sort lines in one pass
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        lines = [line.strip() for line in infile]
        # Sort based on the integer value after the last dot in the filename
        sorted_lines = sorted(lines, key=lambda x: int(x.rsplit('.', 1)[-1]))

        # Write sorted lines to the output file
        outfile.write("\n".join(sorted_lines) + "\n")
