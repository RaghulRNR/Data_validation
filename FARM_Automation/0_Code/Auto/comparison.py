# importing the library;
import os
import difflib
import shutil
from datetime import date
import time


# seperator function
def split(file):
    data = file.read()
    condition_char = "HLCARMCROE"
    i = 0
    e = len(condition_char)
    check_list = list()
    for c in range(len(data)):
        temp = data[i:e]
        if (temp == condition_char):
            check_list.append(i)
        i += 1
        e += 1
    writer = list()
    for i in range(len(check_list)):
        f = len(data)
        c = len(check_list)
        if (i + 1 != c):
            writer.append(data[check_list[i]:check_list[i + 1]])
        else:
            writer.append(data[check_list[i]:f])
    file.close()
    return writer


def file_write(data, path):
    file_2 = open(path, "w")
    for i in range(len(data)):
        file_2.write(data[i])
        file_2.write("\n")
    file_2.close()


def split_write(path):
    main_folder = os.listdir(path)

    for fol in main_folder:
        temp_path = path + "/" + fol
        sub_fol = os.listdir(temp_path)
        for fol in sub_fol:
            temp_path_2 = temp_path + "/" + fol
            files = os.listdir(temp_path_2)
            for file in files:
                if (file == "LIABCL.txt"):
                    split_path = temp_path_2 + "/" + file
                    print(split_path)
                    data_file = open(split_path, "r")
                    write_data = split(data_file)
                    file_write(write_data, split_path)
                    print("> The file ", file, " has been splitted")


# comparision of files:

def converter(folder):
    s_name = list()
    for file in folder:
        s = ""
        for words in file:
            if (int(ord(words)) >= 65 and int(ord(words)) <= 90 or int(ord(words)) >= 97 and int(ord(words)) <= 122):
                s += words
        s_name.append(s)
    return s_name


def list_matching(s_name, o_name, new, old):
    s_count = 0
    n = 0
    output = dict()
    for s_ob in s_name:
        o_count = 0
        temp = list()
        for o_ob in o_name:
            if (s_ob == o_ob):
                n_fol = old[s_count]
                o_fol = new[o_count]
                output[n] = n_fol, o_fol
                n += 1
                break
            o_count += 1
        s_count += 1
    count = 0
    return output


def mismatch_report(new_path, old_path, dict_data, mis_path):
    for i in range(len(dict_data)):
        d_var = date.today()
        t = time.localtime()
        t_var = time.strftime("%H%M%S", t)
        name_append = "_"+str(d_var.year) + str(d_var.month) + str(d_var.day) +"_"+ str(t_var)
        temp = dict_data[i]
        n_files_path = new_path + "/" + temp[0]
        o_files_path = old_path + "/" + temp[1]
        n_files = os.listdir(n_files_path)
        o_files = os.listdir(o_files_path)
        mismatch_path = mis_path + "/" + temp[0]+name_append
        os.mkdir(mismatch_path)

        for n in range(len(n_files)):
            main_file = o_files[n]
            valid_file = n_files[n]
            file_1_path = o_files_path + "/" + main_file
            file_2_path = n_files_path + "/" + valid_file
            main_file_lines = open(file_1_path).readlines()
            valid_file_lines = open(file_2_path).readlines()
            first_list = list()
            second_list = list()
            for i in range(len(main_file_lines)):
                if (main_file_lines[i] != valid_file_lines[i]):
                    first_list.append(main_file_lines[i])
                    second_list.append(valid_file_lines[i])

                    main_file_line = first_list
                    valid_file_line = second_list

                    mismatched_report_path = mismatch_path + "/" + main_file[:-4] + ".html"
                    difference = difflib.HtmlDiff().make_file(main_file_line, valid_file_line, first_list, second_list)
                    difference_report = open(mismatched_report_path, "w")
                    difference_report.write(difference)
                    difference_report.close()


def report_generator(new_path, old_path, dict_data, com_path):
    for i in range(len(dict_data)):
        d_var = date.today()
        t = time.localtime()
        t_var = time.strftime("%H%M%S", t)
        name_append = "_"+str(d_var.year) + str(d_var.month) + str(d_var.day) +"_"+str(t_var)
        temp = dict_data[i]
        n_files_path = new_path + "/" + temp[0]
        o_files_path = old_path + "/" + temp[1]
        n_files = os.listdir(n_files_path)
        o_files = os.listdir(o_files_path)
        report_path = com_path + "/" + temp[0]+name_append
        os.mkdir(report_path)
        for n in range(len(n_files)):
            main_file = o_files[n]
            valid_file = n_files[n]
            file_1_path = o_files_path + "/" + main_file
            file_2_path = n_files_path + "/" + valid_file
            main_file_lines = open(file_1_path).readlines()
            valid_file_lines = open(file_2_path).readlines()
            difference = difflib.HtmlDiff().make_file(main_file_lines, valid_file_lines, file_1_path, file_2_path)
            difference_report = open(report_path + "/" + main_file[:-4] + ".html", "w")
            difference_report.write(difference)
            difference_report.close()
            print("[==>] The comparision report has been generated")


def comparision_report(path, comparision_path, mismatch_path):
    main_folder = os.listdir(path)
    report_path_new = path + "/" + main_folder[0]
    report_path_old = path + "/" + main_folder[1]
    new_fol = os.listdir(report_path_new)
    old_fol = os.listdir(report_path_old)
    old_files = converter(old_fol)
    new_files = converter(new_fol)
    record = list_matching(new_files, old_files, new_fol, old_fol)
    report_generator(report_path_new, report_path_old, record, comparision_path)
    mismatch_report(report_path_new, report_path_old, record, mismatch_path)


def cleaner(file_path, destination_path):
    folders = os.listdir(file_path)
    for f in folders:
        source_path = file_path + "/" + f
        shutil.move(source_path, destination_path)
        print("==> old files moved")


# main execution
def main():
    comparision_path = "C:/Users/i30691/OneDrive - Verisk Analytics/Desktop/Comparison/comparison"
    mismatch_fol_path = "C:/Users/i30691/OneDrive - Verisk Analytics/Desktop/Comparison/mismatch"
    try:
        exp_comparision_path = "C:/Users/i30691/OneDrive - Verisk Analytics/Desktop/Comparison/oldrun_files/comparison"
        exp_mismatch_path = "C:/Users/i30691/OneDrive - Verisk Analytics/Desktop/Comparison/oldrun_files/mismatch"
        cleaner(comparision_path, exp_comparision_path)
        cleaner(mismatch_fol_path, exp_mismatch_path)
    except:
        print(" ")
    main_folder_path = "C:/Users/i30691/OneDrive - Verisk Analytics/Desktop/Comparison/file"
    split_write(main_folder_path)
    comparision_report(main_folder_path, comparision_path, mismatch_fol_path)


main()
print("Execution has been terminated")

