class FaceEncoder:
    def __init__(self, model_name):
        self.model_name = model_name
    def encode(self, obj):
        attr = obj.get_attribute(self.model_name, "feature")
        if attr is not None:
            return attr.values[0].as_floats()
        return None
