class FileAccessError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"Error -> {self.message}"


class KeyStringError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"Error -> {self.message}"
