import pandas as pd
import re
import math
import json


class UserData :

    def __init__(self):
        self.genMajorMap()
        rawUsers = pd.DataFrame.from_csv("./raw.csv", tupleize_cols = True)
        rawUsers['people'] = pd.Series(1.0, index = rawUsers.index)

        self.data = rawUsers

        self.processColumn("Major", True, "/|&|\+|,|and")
        self.processColumn("Marketing")
        self.processColumn("Companies")

        self.data = UserData.expand(self.data, "Major")
        self.data = UserData.expand(self.data, "Marketing")
        self.data = UserData.expand(self.data, "Companies")

        

    def genMajorMap(self):           
        majorsRaw = json.load(open("majorsRaw.json"));
        self.majorMap = dict()
        for finalName, possibleList in majorsRaw.items():
            for major in possibleList:
                self.majorMap[major.lower()] = finalName.lower()

    def processColumn(self, colName, useMajorMap = False, delimeter = "/|&|\+|,") :
        def map(entry):
            if (entry != entry):
                entry = "None"
            split = re.split(delimeter, entry)
            for i in range(len(split)):
                split[i] = split[i].lower().strip()

                if useMajorMap:
                    if split[i] in self.majorMap:
                        split[i] = self.majorMap[split[i]]
            return split

        self.data[colName] = self.data[colName].apply(map)


    @staticmethod
    def expand(df, colName):
        newDataFrameArray = [];
        for index, row in df.iterrows():
            newPeople = row["people"] / len(row[colName])
            for entry in row[colName]:
                newRow = row
                newRow["Timestamp"] = index
                newRow[colName] = entry
                newRow["people"] = newPeople
                newDataFrameArray.append(newRow)
        return pd.DataFrame(newDataFrameArray, columns=df.columns)


    def marketing(self):
        marketing = self.data.groupby("Marketing").sum()
        marketing  = marketing.sort("people", ascending=False)
        return marketing

    def majors(self):
        majors = self.data.groupby("Major").sum()
        majors  = majors.sort("people", ascending=False)
        return majors

    def companies(self):
        companies = self.data.groupby("Companies").sum()
        companies  = companies.sort("people", ascending=False)
        return companies

data = UserData()