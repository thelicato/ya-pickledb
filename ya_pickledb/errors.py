class WrongTypeError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"Error -> {self.message}"


class FileAccessError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"Error -> {self.message}"


class LoadChecksumError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"Error -> {self.message}"
