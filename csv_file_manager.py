from pathlib import Path


class CsvManager:

    def __init__(self):
        self.dataframe = None
        self.default_dir = "data"

    def get_csv_files(self) -> list[Path]:

        """
        To get all csv or text files in a given subdirectory.

        :return: A list of file paths in a given directory.
        :rtype: list[Path].
        """

        files = [file for file in Path.cwd().joinpath(self.default_dir).iterdir() if file.suffix in (".txt", ".csv")]

        return files

    def get_csv_file(self) -> Path | None:

        """
        To get a specific csv file in a given directory.

        :return: A Pandas DataFrame or None if program is keyboard interrupted.
        :rtype: pandas.DataFrame
        """

        files = [file for file in Path.cwd().joinpath(self.default_dir).iterdir() if file.suffix in (".txt", ".csv")]

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
                print("Falsche Eingabe")
            else:
                if custom_index < 0 or custom_index >= len(files):
                    print("Falsche Eingabe")
                else:
                    active = False

        if custom_index in ("q", "Q"):
            return None

        output_file = files[custom_index]
        return output_file
