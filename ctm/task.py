class Task:
    ID = "id"
    TAG_KEY = "tag"
    SAVE_OUTPUT = "save_output"

    def __init__(self, json_definition):
        try:
            self.save_output = json_definition[Task.SAVE_OUTPUT]
        except KeyError:
            # By default if no save output policy is specified we don't save it
            self.save_output = False

        self.id = json_definition[Task.ID]

    def must_be_saved(self):
        return self.save_output
