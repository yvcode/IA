class FaceObjectFilter:
    def is_valid(self, result_obj):
        return result_obj.label == "face"
