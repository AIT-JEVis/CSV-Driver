package org.jevis.csvparser;


/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisType;
import org.jevis.commons.DatabaseHelper;
import org.jevis.commons.driver.Converter;
import org.jevis.commons.driver.ConverterFactory;
import org.jevis.commons.driver.DataCollectorTypes;
import org.jevis.commons.driver.Parser;
import org.jevis.commons.driver.Result;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author broder
 */
public class CSVParser implements Parser {

    private String _quote;
    private String _delim;
    private Integer _headerLines;
    private Integer _dateIndex;
    private Integer _timeIndex;
    private Integer _dpIndex;
    private String _dateFormat;
    private String _timeFormat;
    private String _decimalSeperator;
    private String _thousandSeperator;

    private List<JEVisObject> _dataPoints = new ArrayList<JEVisObject>();
    private List<Result> _results = new ArrayList<Result>();
    private Converter _converter;

    /**
     *
     * @param inputList
     */
    @Override
    public void parse(List<InputStream> inputList) {
        Logger.getLogger(this.getClass().getName()).log(Level.ALL, "Start CSV parsing");
        for (InputStream inputStream : inputList) {

            _converter.convertInput(inputStream);
            String[] stringArrayInput = (String[]) _converter.getConvertedInput(String[].class);
//        String[] stringArrayInput = ic.getStringArrayInput();
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Count of lines" + stringArrayInput.length);

            for (int i = _headerLines; i < stringArrayInput.length; i++) {
                try {
                    //TODO 1,"1,1",1 is not working yet
                    String[] line = stringArrayInput[i].split(String.valueOf(_delim), -1);
                    if (_quote != null) {
                        line = removeQuotes(line);
                    }

                    parseLine(line);
                } catch (Exception e) {
                    Logger.getLogger(this.getClass().getName()).log(Level.WARN, "Detect a Problem in the Parsing Process");
                }
            }
//        Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Number of Results: " + _results.size());
            if (!_results.isEmpty()) {
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, "LastResult (Date,Target,Value): " + _results.get(_results.size() - 1).getDate() + "," + _results.get(_results.size() - 1).getOnlineID() + "," + _results.get(_results.size() - 1).getValue());
            } else {
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Cant parse or cant find any parsable data");
            }
        }
    }

    private void parseLine(String[] line) throws JEVisException {
        DateTime dateTime = getDateTime(line);

        for (JEVisObject dp : _dataPoints) {
            try {
                JEVisClass dpClass = dp.getJEVisClass();

                JEVisType mappingIdentifierType = dpClass.getType(DataCollectorTypes.DataPoint.CSVDataPoint.MAPPING_IDENTIFIER);
                JEVisType targetType = dpClass.getType(DataCollectorTypes.DataPoint.CSVDataPoint.TARGET);
                JEVisType valueIdentifierType = dpClass.getType(DataCollectorTypes.DataPoint.CSVDataPoint.VALUE_INDEX);

                Long datapointID = dp.getID();
                String mappingIdentifier = DatabaseHelper.getObjectAsString(dp, mappingIdentifierType);
                String targetString = DatabaseHelper.getObjectAsString(dp, targetType);
                Long target = null;
                try {
                    target = Long.parseLong(targetString);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                String valueString = DatabaseHelper.getObjectAsString(dp, valueIdentifierType);
                Integer valueIndex = null;
                try {
                    valueIndex = Integer.parseInt(valueString);
                    valueIndex--;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                Boolean mappingError = false;
                try {
                    String currentMappingValue = null;
                    if (_dpIndex != null) {
                        currentMappingValue = line[_dpIndex];
                    }
                    if (mappingIdentifier != null && !currentMappingValue.equals(mappingIdentifier)) {
                        mappingError = true;
                    }
                } catch (Exception ex) {
                    Logger.getLogger(this.getClass().getName()).log(Level.WARN, "This line in the file is not valid: " + line);
                }

                Boolean valueValid = false;
                String sVal = null;
                Double value = null;
                try {
                    sVal = line[valueIndex];
                    if (_thousandSeperator != null && !_thousandSeperator.equals("")) {
                        sVal = sVal.replaceAll("\\" + _thousandSeperator, "");
                    }
                    if (_decimalSeperator != null && !_decimalSeperator.equals("")) {
                        sVal = sVal.replaceAll("\\" + _decimalSeperator, ".");
                    }
                    value = Double.parseDouble(sVal);
                    valueValid = true;
                } catch (Exception nfe) {
                    valueValid = false;
                }

                if (!valueValid) {
                    StringBuilder failureLine = new StringBuilder();
                    for (int current = 0; current < line.length; current++) {
                        failureLine.append(line[current]);
                        if (current < line.length - 1) {
                            failureLine.append(_delim);
                        }
                    }
                    Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Value is not valid in line: " + failureLine.toString());
                    Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Value Index: " + valueIndex);
                    continue;
                }

                if (mappingError) {
                    continue;
                }
                if (dateTime == null) {
                    continue;
                }
                _results.add(new Result(target, value, dateTime));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private String[] removeQuotes(String[] line) {
        String[] removed = new String[line.length];
        for (int i = 0; i < line.length; i++) {
            removed[i] = line[i].replace(_quote, "");
        }
        return removed;
    }

    private DateTime getDateTime(String[] line) {
        String input = "";
        try {
            String date = line[_dateIndex];
            String pattern = _dateFormat;
            input = date;

            if (_timeFormat != null && _timeIndex > -1) {
                String time = line[_timeIndex];
                pattern += " " + _timeFormat;
                input += " " + time;
            }
//            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "complete time " + format);
//            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "complete pattern " + pattern);

            DateTimeFormatter fmt = DateTimeFormat.forPattern(pattern);
            return fmt.parseDateTime(input);
        } catch (Exception ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.WARN, "Date not parsable: " + input);
            Logger.getLogger(this.getClass().getName()).log(Level.WARN, "DateFormat: " + _dateFormat);
            Logger.getLogger(this.getClass().getName()).log(Level.WARN, "DateIndex: " + _dateIndex);
            Logger.getLogger(this.getClass().getName()).log(Level.WARN, "TimeFormat: " + _timeFormat);
            Logger.getLogger(this.getClass().getName()).log(Level.WARN, "TimeIndex: " + _timeIndex);
            Logger.getLogger(this.getClass().getName()).log(Level.WARN, "Exception: " + ex);
        }

        if (_dateFormat == null) {
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "No Datetime found");
            return null;
        } else {
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "Current Datetime");
            return new DateTime();
        }
    }

    @Override
    public List<Result> getResult() {
        return _results;
    }

    @Override
    public void initialize(JEVisObject parserObject) {
        initializeAttributes(parserObject);

        _converter = ConverterFactory.getConverter(parserObject);

        initializeCSVDataPointParser(parserObject);
    }

    private void initializeAttributes(JEVisObject parserObject) {
        try {
            JEVisClass jeClass = parserObject.getJEVisClass();
            JEVisType seperatorColumn = jeClass.getType(DataCollectorTypes.Parser.CSVParser.DELIMITER);
            JEVisType enclosedBy = jeClass.getType(DataCollectorTypes.Parser.CSVParser.QUOTE);
            JEVisType ignoreFirstNLines = jeClass.getType(DataCollectorTypes.Parser.CSVParser.NUMBER_HEADLINES);
            JEVisType dpIndexType = jeClass.getType(DataCollectorTypes.Parser.CSVParser.DATAPOINT_INDEX);
            JEVisType dateIndexType = jeClass.getType(DataCollectorTypes.Parser.CSVParser.DATE_INDEX);
            JEVisType timeIndexType = jeClass.getType(DataCollectorTypes.Parser.CSVParser.TIME_INDEX);
            JEVisType dateFormatType = jeClass.getType(DataCollectorTypes.Parser.CSVParser.DATE_FORMAT);
            JEVisType timeFormatType = jeClass.getType(DataCollectorTypes.Parser.CSVParser.TIME_FORMAT);
            JEVisType decimalSeperatorType = jeClass.getType(DataCollectorTypes.Parser.CSVParser.DECIMAL_SEPERATOR);
            JEVisType thousandSeperatorType = jeClass.getType(DataCollectorTypes.Parser.CSVParser.THOUSAND_SEPERATOR);

            _delim = DatabaseHelper.getObjectAsString(parserObject, seperatorColumn);
            _quote = DatabaseHelper.getObjectAsString(parserObject, enclosedBy);
            _headerLines = DatabaseHelper.getObjectAsInteger(parserObject, ignoreFirstNLines);
            if (_headerLines == null) {
                _headerLines = 0;
            }
            _dpIndex = DatabaseHelper.getObjectAsInteger(parserObject, dpIndexType);
            if (_dpIndex != null) {
                _dpIndex--;
            }
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "DpIndex: " + _dpIndex);

            _dateIndex = DatabaseHelper.getObjectAsInteger(parserObject, dateIndexType);
            if (_dateIndex != null) {
                _dateIndex--;
            }
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "DateIndex: " + _dateIndex);

            _timeIndex = DatabaseHelper.getObjectAsInteger(parserObject, timeIndexType);
            if (_timeIndex != null) {
                _timeIndex--;
            }
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "TimeIndex: " + _timeIndex);

            _dateFormat = DatabaseHelper.getObjectAsString(parserObject, dateFormatType);
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "DateFormat: " + _dateFormat);

            _timeFormat = DatabaseHelper.getObjectAsString(parserObject, timeFormatType);
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "TimeFormat: " + _timeFormat);

            _decimalSeperator = DatabaseHelper.getObjectAsString(parserObject, decimalSeperatorType);
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "DecimalSeperator: " + _decimalSeperator);

            _thousandSeperator = DatabaseHelper.getObjectAsString(parserObject, thousandSeperatorType);
            Logger.getLogger(this.getClass().getName()).log(Level.ALL, "ThousandSeperator: " + _thousandSeperator);

        } catch (JEVisException ex) {
            Logger.getLogger(org.jevis.csvparser.CSVParser.class
                    .getName()).log(Level.ERROR, null, ex);
        }
    }

    private void initializeCSVDataPointParser(JEVisObject parserObject) {
        try {
            JEVisClass dirClass = parserObject.getDataSource().getJEVisClass(DataCollectorTypes.DataPointDirectory.CSVDataPointDirectory.NAME);
            JEVisObject dir = parserObject.getChildren(dirClass, true).get(0);
            JEVisClass dpClass = parserObject.getDataSource().getJEVisClass(DataCollectorTypes.DataPoint.CSVDataPoint.NAME);
            _dataPoints = dir.getChildren(dpClass, true);
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(CSVParser.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }
}
