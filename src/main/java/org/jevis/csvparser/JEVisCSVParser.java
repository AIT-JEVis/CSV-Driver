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

/**
 *
 * @author broder
 */
public class JEVisCSVParser implements Parser {

    private CSVParser _csvParser;

    /**
     *
     * @param inputList
     */
    @Override
    public void parse(List<InputStream> inputList) {
        _csvParser.parse(inputList);
    }

    @Override
    public List<Result> getResult() {
        return _csvParser.getResult();
    }

    @Override
    public void initialize(JEVisObject parserObject) {
        initializeAttributes(parserObject);

        Converter converter = ConverterFactory.getConverter(parserObject);
        _csvParser.setConverter(converter);

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

            String delim = DatabaseHelper.getObjectAsString(parserObject, seperatorColumn);
            String quote = DatabaseHelper.getObjectAsString(parserObject, enclosedBy);
            Integer headerLines = DatabaseHelper.getObjectAsInteger(parserObject, ignoreFirstNLines);
            if (headerLines == null) {
                headerLines = 0;
            }
            Integer dpIndex = DatabaseHelper.getObjectAsInteger(parserObject, dpIndexType);
            if (dpIndex != null) {
                dpIndex--;
            }

            Integer dateIndex = DatabaseHelper.getObjectAsInteger(parserObject, dateIndexType);
            if (dateIndex != null) {
                dateIndex--;
            }

            Integer timeIndex = DatabaseHelper.getObjectAsInteger(parserObject, timeIndexType);
            if (timeIndex != null) {
                timeIndex--;
            }

            String dateFormat = DatabaseHelper.getObjectAsString(parserObject, dateFormatType);

            String timeFormat = DatabaseHelper.getObjectAsString(parserObject, timeFormatType);

            String decimalSeperator = DatabaseHelper.getObjectAsString(parserObject, decimalSeperatorType);

            String thousandSeperator = DatabaseHelper.getObjectAsString(parserObject, thousandSeperatorType);

            _csvParser = new CSVParser();
            _csvParser.setDateFormat(dateFormat);
            _csvParser.setDateIndex(dateIndex);
            _csvParser.setDecimalSeperator(decimalSeperator);
            _csvParser.setDelim(delim);
            _csvParser.setDpIndex(dpIndex);
            _csvParser.setHeaderLines(headerLines);
            _csvParser.setQuote(quote);
            _csvParser.setThousandSeperator(thousandSeperator);
            _csvParser.setTimeFormat(timeFormat);
            _csvParser.setTimeIndex(timeIndex);

        } catch (JEVisException ex) {
            Logger.getLogger(org.jevis.csvparser.JEVisCSVParser.class
                    .getName()).log(Level.ERROR, null, ex);
        }
    }

    private void initializeCSVDataPointParser(JEVisObject parserObject) {
        try {
            JEVisClass dirClass = parserObject.getDataSource().getJEVisClass(DataCollectorTypes.DataPointDirectory.CSVDataPointDirectory.NAME);
            JEVisObject dir = parserObject.getChildren(dirClass, true).get(0);
            JEVisClass dpClass = parserObject.getDataSource().getJEVisClass(DataCollectorTypes.DataPoint.CSVDataPoint.NAME);
            List<JEVisObject> dataPoints = dir.getChildren(dpClass, true);
            List<CSVDataPoint> csvdatapoints = new ArrayList<CSVDataPoint>();
            for (JEVisObject dp : dataPoints) {
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
                CSVDataPoint csvdp = new CSVDataPoint();
                csvdp.setMappingIdentifier(mappingIdentifier);
                csvdp.setTarget(target);
                csvdp.setValueIndex(valueIndex);
                csvdatapoints.add(csvdp);
            }
            _csvParser.setDataPoints(csvdatapoints);
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(JEVisCSVParser.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }
}
