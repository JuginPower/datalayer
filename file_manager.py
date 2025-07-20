from pathlib import Path
import csv
import json


class FileManager:

    """
    The main purpose for this class is to serve as a parent class for different file writing and reading functionality.
    It can be inherited by subclasses like JsonFileManager or CsvFileManager.

    """

    def __init__(self, allowed_suffixes: tuple, default_subdir: str | Path):
        """
        It initializes especially the current work directory with a specific default subdirectory path.

        :param allowed_suffixes: The allowed suffixes that depend on the parameter of the child class.
        :param default_subdir: The default subdirectory where the files should be.
        :type default_subdir: str | Path
        """

        self.default_dir = Path.cwd().joinpath(default_subdir)
        self.suffixes = allowed_suffixes

    def set_default_dir(self, new_path: str | Path):

        """
        Setter for the attribute default_dir.

        :param new_path: The new path of the current work directory
        :type new_path: str | Path
        """

        self.default_dir = Path(new_path)

    def add_subdir(self, additional_path: str | Path):

        """
        Adds a new subdirectory path for the current work directory

        :param additional_path: A new subdirectory path for the current work directory
        :type additional_path: str | Path
        """

        self.default_dir = Path.joinpath(self.default_dir, additional_path)

    def get_files(self) -> list[Path]:

        """
        To get all csv or text files in a given subdirectory.

        :return: A list of file paths in a given directory.
        :rtype: list[Path].
        """

        files = [file for file in Path.joinpath(self.default_dir).iterdir() if file.suffix in self.suffixes]

        return files

    def get_file(self) -> Path | None:

        """
        To get a specific csv file in a given directory.

        :return: A Path object or None if program is keyboard interrupted.
        :rtype: Path
        """

        files = [file for file in Path.cwd().joinpath(self.default_dir).iterdir() if file.suffix in self.suffixes]

        for index, name in enumerate(files):
            print(f"{index} type for:", name.name)

        active = True
        custom_index = 0

        print("You can quit the program with 'q' or 'Q'.")

        while active:
            try:
                custom_index = input("Input: ")
                if custom_index in ("q", "Q"):
                    break

                custom_index = int(custom_index)
            except ValueError:
                print("Wrong Input")
            else:
                if custom_index < 0 or custom_index >= len(files):
                    print("There is no data under the index:", custom_index)
                else:
                    active = False

        if custom_index in ("q", "Q"):
            return None

        output_file = files[custom_index]
        return output_file


class CsvFileManager(FileManager):

    def __init__(self, suffixes = tuple([".txt", ".csv"]), default_subdir="data"):
        super().__init__(suffixes, default_subdir)

    def write_new_csv(self, file_path: str | Path, csv_list):

        """
        This method writes a python list of records to the specified path on disk.
        It doesn't check the structure of the data.

        :param file_path: The path to the file.
        :param csv_list: The data which will be written.
        :return: True if it's done for awaiting purposes.
        """

        with open(Path().joinpath(self.default_dir.parent, file_path), 'w', newline='',
                  encoding="ISO-8859-1") as csvfile:

            csv_writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

            for line in csv_list:
                csv_writer.writerow(line)

        return True


class JsonFileManager(FileManager):

    def __init__(self, suffixes = tuple([".json", ".jsonl"]), default_subdir="data"):
        super().__init__(suffixes, default_subdir)

    def write_new_json(self, file_path: str | Path, json_data: str | dict):

        """
        This method writes a python dictionary or jsonified string of records to the specified path on disk.
        It doesn't check the structure of the data.

        :param file_path: The path to the file.
        :param json_data: The data which will be written. It can handel json_string and also python dictionaries
        :return: True if it's done for awaiting purposes.
        """

        with open(Path().joinpath(self.default_dir.parent, file_path), "w") as jsonfile:

            json.dump(json_data, jsonfile)

        return True