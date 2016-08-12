#!/usr/bin/python
#   sis00024@students.stir.ac.uk
#   Sivakumaran Somasundaram, Student Number - 2426734
#   ITNPBD2 Assignment 2015
################################################################################
#   Load Modules
import platform     #print platform.system(), platform.release()
import os           #File existence checks
import sys          #File parameter checks
import json
import csv
import math
import re
import time
################################################################################
sInputFile = ''; sMetaFile = ''; sFileFormat = '';
sHeaderPresent = ''; sFieldSeparator = ''
nNumberofFields = 0
nNumericFields  = 0
nStringFields   = 0
dictFieldTypes  = {}     #Dictionary holding Field name and type (String/Numeric)
listFullData    = []     #Main datastructure for holding data in a table
dictJSONOutput  = {}     #Main structure for holding metadata in a dictionary
if len(sys.argv) > 1:
    if sys.argv[1] == '-h' or sys.argv[1] == '--help':
        print 'Usage: ./PyProc.py param.json'
        exit(0)         #Always exit gracefully
    else:
        sConfigFile = str(sys.argv[1])
else:
    print 'This program needs a parameter file to run. Please provide the name of the configuration file'
    print 'Usage: ./PyProc.py param.json'
    print 'Exiting...'
    exit(0)             #Always exit gracefully
############################Functions###########################################
def findFileType(firstLine):
    """Determine File Type.

    Based on meeting the exclusive json criteria of a '{' in the beginning and
    a '}' in the end of a json object.
    """
    match = re.search(r'^{.+}$', firstLine.rstrip())
    if match:
        dictJSONOutput['filetype'] = 'JSON'
        return 'JSON'
    else:
        dictJSONOutput['filetype'] = 'CSV'
        return 'CSV'
def findFieldTypes(inputrow):
    """Find the field types by catching exceptions.

    A dictionary containing the first row of data is passed to this function.
    Exceptions to typecasting each of the value are caught to determine whether
    a field is Numeric or String. NOT USED
    """
    global nNumericFields
    global nStringFields
    for key, value in inputrow.iteritems():
        try:
            v = float(value)
            dictFieldTypes[key] = 'Numeric'
            nNumericFields += 1
        except:
            dictFieldTypes[key] = 'String'
            nStringFields += 1
    nNumberofFields = len(dictFieldTypes)
    dictJSONOutput['No Of Fields'] = nNumberofFields
def findFieldTypesRegex(inputrow):
    """Find the field types using RegEx.

    A dictionary containing the first row of data is passed to this function.
    RegEx is used to match against the pattern as below. All integers, floats,
    positive and negative, Scientific Notation values (12E4) are cleared as
    Numeric. An example of fullest match would be -1234.45E12.
    """
    global nNumericFields
    global nStringFields
    #       Best regex exp I created for numeric - ^[-+]?\d*\.*\d+E*[-+]?\d*$
    #       Worst case string - -0.0002E-32
    for key, value in inputrow.iteritems():
        result = re.search(r'^[-+]?\d*\.*\d+E*[-+]?\d*$', str(value))
        if result:
            dictFieldTypes[key] = 'Numeric'
            nNumericFields += 1
        else:
            dictFieldTypes[key] = 'String'
            nStringFields += 1
    nNumberofFields = len(dictFieldTypes)
    dictJSONOutput['No Of Fields'] = nNumberofFields
def calculateMedian(inputList):
    """Find the Median of the numeric input list."""
    l = sorted(inputList)
    if len(l) % 2 == 0: med = (l[(len(l)/2)-1] + l[(len(l)/2)])/2.0
    else: med = 1.0*l[len(l)/2]
    return med
def calculateRange(inputList):
    """Find the Range of the numeric input list."""
    inputList = list(inputList)
    return max(inputList) - min(inputList)
def calculateMean(inputList):
    """Calculate the mean (average) of the numeric input list."""
    inputList = list(inputList)
    assert len(inputList) > 0
    return sum(inputList)/len(inputList)
def calculateQ1(inputList):
    """Calculate the First Quartile of the numeric input list."""
    inputList = sorted(list(inputList))
    lFirstHalf = inputList[:len(inputList)/2]
    return calculateMedian(lFirstHalf)
def calculateQ3(inputList):
    """Calculate the Third Quartile of the numeric input list."""
    inputList = sorted(list(inputList))
    lSecondHalf = inputList[len(inputList)/2+1:]
    return calculateMedian(lSecondHalf)
def calculateHistogram(inputList, fieldname):
    """Calculate and plot the Histogram for the numeric input list.

    The Histogram provides an easy, visually evident, distribution of data
    according to pre-determined slots or bins. In this function, there are 10
    bins. The range of the data is therefore divided into 10 bins and the number
    of occurances in each of the bin are printed as a '#'.

    Although this metadata is useful, it is not written to the metadata output
    file. Rather, it is just printed to the screen for perusal.

    Print formatting is based on this (MacBook Air 13") screen. May go awry in
    other screens / resolution or if the number of occurances are more than the
    approximately 175; The '#' characters will wrap to the next line distorting
    the appearance
    """
    inputList = list(inputList)
    nBins = 10
    nBinValues = [0,0,0,0,0,0,0,0,0,0]
    fRange = calculateRange(inputList)
    fBinWidth = fRange/nBins
    for i in inputList:
        if min(inputList) <= i <= min(inputList)+fBinWidth:
            nBinValues[0] += 1
        if min(inputList)+fBinWidth < i <= min(inputList)+2*fBinWidth:
            nBinValues[1] += 1
        if min(inputList)+2*fBinWidth < i <= min(inputList)+3*fBinWidth:
            nBinValues[2] += 1
        if min(inputList)+3*fBinWidth < i <= min(inputList)+4*fBinWidth:
            nBinValues[3] += 1
        if min(inputList)+4*fBinWidth < i <= min(inputList)+5*fBinWidth:
            nBinValues[4] += 1
        if min(inputList)+5*fBinWidth < i <= min(inputList)+6*fBinWidth:
            nBinValues[5] += 1
        if min(inputList)+6*fBinWidth < i <= min(inputList)+7*fBinWidth:
            nBinValues[6] += 1
        if min(inputList)+7*fBinWidth < i <= min(inputList)+8*fBinWidth:
            nBinValues[7] += 1
        if min(inputList)+8*fBinWidth < i <= min(inputList)+9*fBinWidth:
            nBinValues[8] += 1
        if min(inputList)+9*fBinWidth < i <= max(inputList):
            nBinValues[9] += 1
    print 'Distribution Histogram for', fieldname, '(Bins = ', nBins, '\b)'
    print 'Range\t\t\t', 'Occurances'
    for i in range(nBins):
        strtemp = str(min(inputList)+i*fBinWidth) + '-' + str(min(inputList)+(i+1)*fBinWidth)
        print   strtemp.rjust(15), ':', str('#'*nBinValues[i]).rjust(0)
    return None
def calculateDiscreteHistogram(inputList, fieldname):
    """
    Calculate and plot a histogram for discrete variable

    The inputs to this function are a list of discrete values and the field's
    name. The Histogram provides an easy, visually evident, distribution of data
    according to the unique values. The number of occurances in each of the bin
    are printed as a '#'.

    Although this metadata is useful, it is not written to the metadata output
    file. Rather, it is just printed to the screen for perusal.

    Print formatting is based on this (MacBook Air 13") screen. May go awry in
    other screens / resolution or if the number of occurances are more than the
    approximately 175; The '#' characters will wrap to the next line distorting
    the appearance
    """
    inputList = list(inputList)
    dictDist = {}
    for i in inputList:
        if i in dictDist:
            dictDist[i] += 1
        else:
            dictDist[i] = 1
    print 'Distribution Histogram for', fieldname
    print 'Value\t\t\t', 'Occurances'
    for k, v in dictDist.iteritems():
        print str(k).ljust(12), ':'.rjust(0), str('#'*v).rjust(0)
    return None
def calculateMode(inputList):
    """Calculate the mode of the numeric input list.

    The mode is the most frequently occuring value in the list. Although the
    function is being called only for numeric data, it is equally applicable
    for string data. In case the data is non-modal, the function returns the
    string 'No Mode'. In case of a multi-modal dataset, one of them is returned.
    """
    inputList = list(inputList)
    dictMode = {}
    maxvalue = 0
    for i in inputList:
        if i in dictMode:
            dictMode[i] += 1
        else:
            dictMode[i] = 1
    for key in dictMode:
        maxvalue = max(maxvalue, dictMode[key])
    if maxvalue == 1:
        return 'No Mode'
    else:
        return max(dictMode, key=dictMode.get)
def calculateVariance(inputList):
    """Calculate the variance of the numeric input list.

    Variance is the average squared deviation from the population mean.
    """
    inputList = list(inputList)
    mean = calculateMean(inputList)
    variance = 0.0
    for i in inputList:
        variance += math.pow((mean - i),2)
    variance /= len(inputList)
    return variance
def calculatePCC(inputList1, inputList2):
    """Calculate the Pearson's Correlation Coefficient for two numeric lists.

    The Pearson's Correlation Coefficient (denoted by r) is a measure of the
    linear correlation between two variables X and Y, giving a value between +1
    and minus 1 inclusive, where 1 is total positive correlation, 0 is no
    correlation, and minus 1 is total negative correlation. It is widely used in
    the sciences as a measure of the degree of linear dependence between two
    variables.
    Inferring the Pearson\'s Correlation Coefficient Ratio (r):
    r >=	+.70 	    Very strong positive relationship
    r +.40 to +.69	    Strong positive relationship
    r +.30 to +.39	    Moderate positive relationship
    r +.20 to +.29	    Weak positive relationship
    r +.01 to +.19	    No or negligible relationship
    r -.01 to -.19	    No or negligible relationship
    r -.20 to -.29	    Weak negative relationship
    r -.30 to -.39	    Moderate negative relationship
    r -.40 to -.69	    Strong negative relationship
    r -.70 or lower     Very strong negative relationship
    """
    inputList1 = list(inputList1)
    inputList2 = list(inputList2)
    assert len(inputList1) == len(inputList2)
    mean_x = calculateMean(inputList1)
    mean_y = calculateMean(inputList2)
    sigmaproduct = 0.0
    sigmaXSquare = 0.0
    sigmaYSquare = 0.0
    for i in range(len(inputList1)):
        sigmaproduct += ( (inputList1[i] - mean_x) * (inputList2[i] - mean_y)  )
        sigmaXSquare += math.pow(   (inputList1[i]-mean_x), 2    )
        sigmaYSquare += math.pow(   (inputList2[i]-mean_y), 2    )
    return sigmaproduct / math.sqrt(sigmaXSquare*sigmaYSquare)
def processData():
    """Calculate all metadata.

    The fundamental approach is to iterate through the list of lists (which is
    already populated with the data), find the type of the field (String or
    Numeric) and then process the slice to find metadata.

    The first row in the table is the field names (from header/user in case of csv)
    The second row in the table is the type of data (String or Numeric)
    The third row onwards in the table is data (regardless of whether datafile is csv or json)
    """
    listMetadata = []
    dictNumericSeries = {}
    for i in range(len(dictFieldTypes)):
        if listFullData[1][i] == 'String':  #If String
            lSeries = [row[i] for row in listFullData[2:]]
            listMetadata.append({'Name':listFullData[0][i], 'Type': 'String',
                'NumUniqueValues': len(set(lSeries)), 'UniqueValues':
                list(set(lSeries)), 'Mode': calculateMode(lSeries)})
            calculateDiscreteHistogram(lSeries, listFullData[0][i]) #Show on screen only
        else:                               #If Numeric
            #To cater for blanks. Uncomment block below if blanks in Numeric fields
            '''
            for row in listFullData[2:]:
                if row[i] == None or row[i] == '':
                    row[i] = 0
            '''
            lSeries = [float(row[i]) for row in listFullData[2:]]
            dictNumericSeries[listFullData[0][i]] = lSeries     #For PCC
            listMetadata.append({
                'Name':listFullData[0][i],
                'Type':'Numeric',
                'Minimum':min(lSeries),
                'Maximum':max(lSeries),
                'Range':calculateRange(lSeries),
                'Average': calculateMean(lSeries),
                'Median':calculateMedian(lSeries),
                'Mode':calculateMode(lSeries),
                'Variance':calculateVariance(lSeries),
                'Standard Deviation':math.sqrt(calculateVariance(lSeries)),
                'First Quartile':calculateQ1(lSeries),
                'Third Quartile': calculateQ3(lSeries),
                'IQR': calculateQ3(lSeries)-calculateQ1(lSeries),
            })
            calculateHistogram(lSeries, listFullData[0][i])         #Show on screen only
        dictJSONOutput['Fields'] = listMetadata
    #Completed processing in the for loop
    ###############Find PCC#####################################################
    if len(dictNumericSeries) == 2: #Find PCC if there are two and only two numeric fields
        lTemp = []
        for key in dictNumericSeries:
            lTemp.append(key)
        x = calculatePCC(dictNumericSeries[lTemp[0]], dictNumericSeries[lTemp[1]])
        sTemp = 'Pearsons CC (' + str(lTemp[0]) + ' Vs ' + str(str(lTemp[1])) + ')'
        dictJSONOutput[sTemp] = x
    if len(dictNumericSeries) > 2:  #Ask the user what fields they want to find PCC between
        lTemp = []
        for key in dictNumericSeries:
            lTemp.append(key)
        print 'There are', len(dictNumericSeries), 'Numeric fields in this data. They are as follows: '
        for f in range(len(lTemp)):
            print f, ':', lTemp[f]
        user_input1 = raw_input('Please enter a number for the first series:\t')
        user_input2 = raw_input('Please enter a number for the second series:\t')
        try:
            x = calculatePCC(dictNumericSeries[lTemp[int(user_input1)]], dictNumericSeries[lTemp[int(user_input2)]])
            sTemp = 'Pearsons CC (' + str(lTemp[int(user_input1)]) + ' Vs ' + str(str(lTemp[int(user_input2)])) + ')'
            dictJSONOutput[sTemp] = x
        except:
            print 'Invalid inputs detected! Please provide valid inputs next time.'
def getUserDefinedVariableNames(inputLine):
    """Get user defined field names in case header is not present in a csv file

    In a CSV file, if a header is not present, default field names have to
    be assigned. In this function, an added feature to prompt the user for a
    variable name is provided. To help the user, a sample data of the field type
    is displayed to provide a meaningful name for the field. Or the user could
    choose to press enter and assign a default name as var0, var1, etc.
    """
    lValues = inputLine.rstrip().split(',')
    print 'There are', len(lValues), 'fields in this data file and their names are not known.'
    lUserDefinedFieldNames = []
    for i in range(len(lValues)):
        sDefaultFieldName = "var%d" % (i,)
        sPrompt = "Please provide a name for the first field. Sample data is %s. Press Enter for default (var%d):\t\t" % (str(lValues[i]), i)
        user_input = raw_input(sPrompt)
        if user_input == '':
            lUserDefinedFieldNames.append(sDefaultFieldName)
        else:
            lUserDefinedFieldNames.append(user_input)
    return ','.join(lUserDefinedFieldNames)
#####################End of Functions###########################################
try:                                #Go ahead and open the configuration file
    with open(sConfigFile, 'r') as f:
        x = json.load(f)
        sInputFile = x['infile']
        if 'metafile' in x:
            sMetaFile = x['metafile']
        else:
            sMetaFile = ''
        if 'format' in x:
            #sFileFormat = x['format']  #Dont go by what the user claims. Check.
            sFileFormat = 'Unknown'
            dictJSONOutput['filetype'] = sFileFormat    #If user has provided. Trust, but verify.
        else:
            sFileFormat = 'Unknown'
        if 'hasheader' in x:
            sHeaderPresent = x['hasheader']
            dictJSONOutput['HeaderPresent'] = 'True'    #If user has provided. Trust, but verify.
        else:
            sHeaderPresent = 'Unknown'
        if 'separator' in x:
            sFieldSeparator = x['separator']
            dictJSONOutput['FieldSeparator'] = sFieldSeparator  #If user has provided. Trust, but verify.
        else:
            sFieldSeparator = 'Unknown'
        dictJSONOutput['inputfile'] = sInputFile
except:     #Inform the user about the file's absence and gracefully exit
    print 'The file', sConfigFile, 'does not exist!'
    exit(0)         #Always exit gracefully
################################################################################
if not os.path.isfile(sInputFile):  #If input file is not present, inform the user and gracefully exit
    print 'The file', sInputFile, 'does not exist. Please check your conf in',\
    sConfigFile, 'Exiting...'
    exit(0)         #Always exit gracefully
if sFileFormat == 'Unknown':        #Always called.
    with open(sInputFile, 'r') as f:
        firstLine = f.readline()
    sFileFormat = findFileType(firstLine)
if sFileFormat.upper() == 'CSV' or sFileFormat.upper() == 'TABULAR':
    """
    Process CSV / Tab Delimited / Any Delimited Text File

    The csv Sniffer class is used to determine the presence of a header as well
    as the separator(delimiter). This may not work if all fields are characters
    To determine if a field is numeric or string, we use a regex function,
    although catching exceptions to an enforced float typecast could be used to
    determine strings. Both work
    """
    with open(sInputFile, 'r') as f:
        if b'\x00' in f.read():
            print 'The file is probably non text. Please check the param file'
            exit(0)     #Always exit gracefully
        f.seek(0)
        dialect = csv.Sniffer().sniff(f.read(1024))
        dictJSONOutput['FieldSeparator'] = dialect.delimiter
        f.seek(0)
        bHeaderPresent = csv.Sniffer().has_header(f.read(1024))
        f.seek(0)
        if bHeaderPresent:  #If header present, find field types
            dictJSONOutput['HeaderPresent'] = 'True'
            r = csv.DictReader(f, dialect = dialect)
            for row in r:
                findFieldTypesRegex(row)
                break
        else:               #If header absent, assign field names & find field types
            dictJSONOutput['HeaderPresent'] = 'False'
            fl = f.readline().rstrip()
            s = getUserDefinedVariableNames(fl)
            fl = fl.split(',')
            s = s.split(',')
            row = dict(zip(list(s), list(fl)))
            findFieldTypesRegex(row)
            listFullData.insert(0, s)
        f.seek(0)
        r=csv.reader(f, dialect=dialect)
        for row in r:
            #Create a list of lists, aka, a table.The first row contains the
            #header, the second row contains the type of data (Numeric/String)
            #Beyond that, all data.
            listFullData.append(row)
        listTemp = []
        for strng in listFullData[0]:
            listTemp.append( dictFieldTypes[strng] )
        listFullData.insert(1, listTemp)
        processData()
if sFileFormat.upper() == 'JSON':
    """
    Process JSON File.

    The JSON file is processed and filled into the list of list (table) and
    processData() is called. Caveat - The JSON file has to be one json object
    per line (newline) and not a jsonarray of json objects. This is because the
    sample data file is crafted this way.
    """
    with open(sInputFile, 'r') as f:
        for line in f:
            jsonData = json.loads(line)
            if not dictFieldTypes:  findFieldTypesRegex(jsonData)
            lRow = []
            lFields = []
            lTypes = []
            for key, value in dictFieldTypes.iteritems():
                lFields.append(key.encode('utf-8'))
                lTypes.append(value.encode('utf-8'))
                if value == 'String':
                    if type(jsonData[key]) == dict:    #If another level of json
                        lRow.append(str(jsonData[key])) #Just leave it, dont go deeper.
                    else:
                        lRow.append(jsonData[key].encode('utf-8'))
                else:
                    lRow.append(jsonData[key])
            listFullData.append(lRow)
        listFullData.insert(0, lFields)
        listFullData.insert(1, lTypes)
        processData()
if sMetaFile:   #If a metadata output file is specified, write to file
    print 'Metadata written to ', sMetaFile
    with open(sMetaFile, 'w') as f:
        json.dump(dictJSONOutput,  f, indent=4)
else:           #Else, write to screen. Neatly.
    print 'Output File name not mentioned. Printing to screen'
    time.sleep(1)
    print json.dumps(dictJSONOutput, indent=4)
'''
References:
CSV Sniffer:    #https://docs.python.org/2/library/csv.html#csv.Sniffer
https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
http://faculty.quinnipiac.edu/libarts/polsci/Statistics.htmlr
Lifesaver:http://stackoverflow.com/questions/209840/map-two-lists-into-a-dictionary-in-python
https://www.python.org/dev/peps/pep-0257/#what-is-a-docstring
'''
