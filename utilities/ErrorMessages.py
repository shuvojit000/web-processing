from enum import Enum

class ErrorMessage(Enum):
    SOURCE_SUCCESS = "Successfully pulled file"
    SOURCE_FAILURE = "Error pulling file"
    TRANSFORMATION_SUCCESS = "Successfully processed reference number"
    TRANSFORMATION_FAILURE = "Error processing reference number"
    TARGET_SUCCESS = "Successfully wrote to"
    TARGET_FAILURE = "Error writing file"


class Status(Enum):
    SUCCESS = "Success"
    FAILURE = "Failure"

