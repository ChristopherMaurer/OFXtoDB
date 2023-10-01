import OFXWriter
class TypeWriter(OFXWriter.Writer):
    def __init__(self):
        self.OFXListDict = {"SECLIST": 0, "INVPOSLIST": 0, "INVTRANLIST": 0, "BANKTRANLIST": 0, "BANKTRANLISTP": 0,
                            "LOANTRANLIST": 0, "AMRTTRANLIST": 0, "CLOSING": 0}
    def OFXListStart(self, list):
        print("List "+list)
    def OFXListEnd(self):
        print()
        return (0, 0)
    def OFXRecStart(self):
        return
    def OFXRecEnd(self):
        print()
        return (1, 0)
    def OFXPutData(self, tag, value, parent):
        print(tag+"="+value, end=", ")
