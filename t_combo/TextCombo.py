import argparse
import json
import re
import sys


def process_multiple_files(config_file):
    configs = open(config_file, "r").read()
    config_dict = json.loads(configs)
    for config in config_dict["configs"]:
        process_single_file(config["input_file"], config["output_file"])


def process_single_file(input_file, output_file):
    text_combo = TextCombo(input_file)
    f = open(output_file, "w")
    f.write(text_combo.substitute())
    f.close()
    print(
        "Input file \"%s\" is successfully processed and generated the output file \"%s\"" % (input_file, output_file))


class TextCombo:
    def __init__(self, file_name):
        self.file_name = file_name
        self.value_dict = dict()
        self.argument_separator = ","
        self.argument_value_separator = ":"
        self.combination_identifier = "({.+?})"
        self.text_replacer_arg = "function"

    # Read the file content
    def read_file(self):
        return open(self.file_name, "r").read()

    # Build the dictionary for arguments and its values
    def build_dict(self, combination_string):
        arg_value_dict = {}
        for string in combination_string[1:len(combination_string) - 1].split(self.argument_separator):
            arg_value_dict[string.split(self.argument_value_separator)[0]] = \
                string.split(self.argument_value_separator)[1]
        return arg_value_dict

    # Substitute and combine texts
    def substitute(self):
        file_content = self.read_file()
        for combination_string in re.findall(self.combination_identifier, file_content):
            if bool(re.search(self.text_replacer_arg, combination_string)):
                temp_arg_value_dict = self.build_dict(combination_string)
                temp_combo = TextCombo(temp_arg_value_dict[self.text_replacer_arg])
                temp_combo.value_dict = temp_arg_value_dict
                file_content = file_content.replace(combination_string, temp_combo.substitute())
            else:
                for string in combination_string[1:len(combination_string) - 1].split(self.argument_separator):
                    replacement = string.split(self.argument_value_separator)[1]
                    if replacement not in self.value_dict.keys():
                        continue
                    file_content = file_content.replace("{" + string + "}", self.value_dict[replacement])
        return file_content


def main(args):
    parser = argparse.ArgumentParser(description="Combine text files.")
    parser.add_argument("-i", "--input_file", type=str, required=False, help='Input file containing text')
    parser.add_argument("-o", "--output_file", type=str, required=False,
                        help='Output file to be generated combining text')
    args = parser.parse_args(args)
    x(args.xcenter, args.ycenter)


if __name__ == '__main__':
    main(sys.argv[1:])
