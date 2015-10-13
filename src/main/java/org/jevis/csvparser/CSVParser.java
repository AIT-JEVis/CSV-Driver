/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.csvparser;

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
import org.jevis.commons.driver.Result;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author broder
 */
public class CSVParser implements Parser {

    // interfaces
    interface CSV extends DataCollectorTypes.Parser {

        public final static String NAME = "CSV Parser";
        public final static String DATAPOINT_INDEX = "Datapoint Index";
        public final static String DATE_INDEX = "Date Index";
        public final static String DELIMITER = "Delimiter";
        public final static String NUMBER_HEADLINES = "Number Of Headlines";
        public final static String QUOTE = "Quote";
        public final static String TIME_INDEX = "Time Index";
        public final static String DATE_FORMAT = "Date Format";
        public final static String DECIMAL_SEPERATOR = "Decimal Separator";
        public final static String TIME_FORMAT = "Time Format";
        public final static String THOUSAND_SEPERATOR = "Thousand Separator";
    }

    interface CSVDataPointDirectory extends DataCollectorTypes.DataPointDirectory {

        public final static String NAME = "CSV Data Point Directory";
    }
    
    interface CSVDataPoint extends DataCollectorTypes.DataPoint {

        public final static String NAME = "CSV Data Point";
        public final static String MAPPING_IDENTIFIER = "Mapping Identifier";
        public final static String VALUE_INDEX = "Value Index";
        public final static String TARGET = "Target";
    }

    // member variables
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

    private List<Result> _results = new ArrayList<Result>();
    private List<CSVDataPoint> _dataPoints = new ArrayList<CSVDataPoint>();
    private Converter _converter;


    public void parse(List<InputStream> inputList) {
        Logger.getLogger(this.getClass().getName()).log(Level.ALL, "Start CSV parsing");
        for (InputStream inputStream : inputList) {

            _converter.convertInput(inputStream);
            String[] stringArrayInput = (String[]) _converter.getConvertedInput(String[].class);
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

        for (CSVDataPoint dp : _dataPoints) {
            try {
                String mappingIdentifier = dp.getMappingIdentifier();
                Integer valueIndex = dp.getValueIndex();
                Long target = dp.getTarget();

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

    public List<Result> getResult() {
        return _results;
    }

    public void setQuote(String _quote) {
        this._quote = _quote;
    }

    }

    public void setDelim(String _delim) {
        this._delim = _delim;
    }

    public void setDateIndex(Integer _dateIndex) {
        this._dateIndex = _dateIndex;
    }

    public void setTimeIndex(Integer _timeIndex) {
        this._timeIndex = _timeIndex;
    }

    public void setDpIndex(Integer _dpIndex) {
        this._dpIndex = _dpIndex;
    }

    public void setDateFormat(String _dateFormat) {
        this._dateFormat = _dateFormat;
    }

    public void setTimeFormat(String _timeFormat) {
        this._timeFormat = _timeFormat;
    }

    public void setDecimalSeperator(String _decimalSeperator) {
        this._decimalSeperator = _decimalSeperator;
    }

    public void setThousandSeperator(String _thousandSeperator) {
        this._thousandSeperator = _thousandSeperator;
    }

    public void setDataPoints(List<CSVDataPoint> _dataPoints) {
        this._dataPoints = _dataPoints;
    }

    public void setConverter(Converter _converter) {
        this._converter = _converter;
    }
    
    
}
    public void setHeaderLines(Integer _headerLines) {
        this._headerLines = _headerLines;
