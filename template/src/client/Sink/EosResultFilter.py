class EosResultFilter:
    def is_valid(self, result):
        if result.eos:
            return False
        return True
