"""
base = 4
depth 0 files 1302 folders 1 number 1302 remain 0
depth 1 files 5209 folders 4 number 1302 remain 1
depth 2 files 20836 folders 16 number 1302 remain 4
depth 3 files 83344 folders 64 number 1302 remain 16
depth 4 files 333376 folders 256 number 1302 remain 64
depth 5 files 1333506 folders 1024 number 1302 remain 258
depth 6 files 5334025 folders 4096 number 1302 remain 1033
depth 7 files 21336102 folders 16384 number 1302 remain 4134
depth 8 files 3552297 folders 65536 number 54 remain 13353
folder: 1 file: 1302
folder: 5 file: 6510
folder: 21 file: 27342
folder: 85 file: 110670
folder: 341 file: 443982
folder: 1365 file: 1777230
folder: 5461 file: 7110222
folder: 21845 file: 28442190
folder: 87381 file: 31981134
new_ratio: [4.069538600276885e-05, 0.0001627815440110754, 0.0006511261760443016, 0.0026045047041772062, 0.010418018816708825, 0.0416720752668353, 0.1666883010673412, 0.6667532042693648, 0.11100929276951449]

base = 5
depth 0 files 291 folders 1 number 291 remain 0
depth 1 files 1456 folders 5 number 291 remain 1
depth 2 files 7281 folders 25 number 291 remain 6
depth 3 files 36409 folders 125 number 291 remain 34
depth 4 files 182048 folders 625 number 291 remain 173
depth 5 files 910242 folders 3125 number 291 remain 867
depth 6 files 4551212 folders 15625 number 291 remain 4337
depth 7 files 22756060 folders 78125 number 291 remain 21685
depth 8 files 3554996 folders 390625 number 9 remain 39371
folder: 1 file: 291
folder: 6 file: 1746
folder: 31 file: 9021
folder: 156 file: 45396
folder: 781 file: 227271
folder: 3906 file: 1136646
folder: 19531 file: 5683521
folder: 97656 file: 28417896
folder: 488281 file: 31933521
new_ratio: [9.102424325931854e-06, 4.5512121629659267e-05, 0.00022756060814829634, 0.0011378030407414815, 0.005689015203707409, 0.028445076018537043, 0.1422253800926852, 0.7111269004634261, 0.11109365002679898]

base = 6
depth 0 files 84 folders 1 number 84 remain 0
depth 1 files 507 folders 6 number 84 remain 3
depth 2 files 3045 folders 36 number 84 remain 21
depth 3 files 18275 folders 216 number 84 remain 131
depth 4 files 109655 folders 1296 number 84 remain 791
depth 5 files 657930 folders 7776 number 84 remain 4746
depth 6 files 3947583 folders 46656 number 84 remain 28479
depth 7 files 23685499 folders 279936 number 84 remain 170875
depth 8 files 3577417 folders 1679616 number 2 remain 218185
folder: 1 file: 84
folder: 7 file: 588
folder: 43 file: 3612
folder: 259 file: 21756
folder: 1555 file: 130620
folder: 9331 file: 783804
folder: 55987 file: 4702908
folder: 335923 file: 28217532
folder: 2015539 file: 31576764
new_ratio: [2.6440752678833838e-06, 1.5864451607300305e-05, 9.518670964380183e-05, 0.0005711202578628109, 0.0034267215471768656, 0.020560329283061195, 0.12336197569836715, 0.740171854190203, 0.11179430378680998]
"""




def forward(file_ratios = [0.001, 0.05, 0.05, 0.1, 0.1, 0.1, 0.3, 0.1, 0.1], folder_base=4):
    depth = 8
    items = 32000000
    folders_per_depth = [0] * (depth + 1)
    file_ratios_sum = sum(file_ratios)

    for i, ratio in enumerate(file_ratios):
        file_ratios[i] /= file_ratios_sum
    
    print(file_ratios)

    for i in range(depth + 1):
        file_per_depth = int(items * file_ratios[i])
        folder_per_depth = folder_base ** i
        folders_per_depth[i] = file_per_depth // folder_per_depth
        left = file_per_depth - folders_per_depth[i] * folder_per_depth
        print(f"depth {i} files {file_per_depth} folders {folder_per_depth} number {folders_per_depth[i]} remain {left}")

    for i in range(depth + 1):
        if i == 0:
            folder_stride = 1
            file_stride = folders_per_depth[i]
        else:
            folder_stride = folder_stride + folder_base ** i
            file_stride = file_stride + folders_per_depth[i] * folder_base ** i
        print(f"folder: {folder_stride} file: {file_stride}")


if __name__ == "__main__":
    # change the files of the last depth
    depth8_files = 3551609
    depth = 8
    items = 32000000
    folders_per_depth = [0] * (depth + 1)
    folder_base = 5

    sum_folders = 0
    for i in range(depth):
        folder_per_depth = folder_base ** i
        sum_folders += folder_per_depth
        # print(f"folder_per_depth[{i}]: {folder_per_depth}")
    # print("folders", sum_folders)

    files_per_folder = (items - depth8_files) // sum_folders
    print("files_per_folder:", files_per_folder)
    new_ratios = []
    for i in range(depth):
        folders = folder_base ** i
        files = files_per_folder * folders
        ratio = files / items
        new_ratios.append(ratio)
    new_ratios.append(depth8_files / items)

    forward(new_ratios, folder_base)
    print("new_ratio:", new_ratios)
