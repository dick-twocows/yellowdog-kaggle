package org.twocows.yellowdog.kaggle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.opencsv.CSVReader;

/**
 * Class to produce JSON serialized lines for Kafka.
 * 
 * https://www.kaggle.com/unitednations/global-commodity-trade-statistics/downloads/commodity_trade_statistics_data.csv
 * 
 * @author dick
 *
 */
public class KaggleLineProducer extends SimpleKafkaProducer<Long, KaggleLine> {

	public static final String CLIENT_ID_CONFIG = "KaggleLineProducer";

	public static final String TOPIC_DEFAULT = "KaggleLine";
	
	public static final String CSV_READER_SPEC = "kaggle.csvreader.spec";
	
	public static final String CSV_READER_GZIP = "kaggle.csvreader.gzip";

	public static KaggleLine START = new KaggleLine(0, new String[0]);

	public static KaggleLine END = new KaggleLine(-1, new String[0]);
	
	public static Properties append(final Properties properties, final String spec, final Boolean gzip) {
		properties.setProperty(CSV_READER_SPEC, spec);
		properties.setProperty(CSV_READER_GZIP, gzip.toString());
		return properties;
	}
	
	private CSVReader csvReader = null;

	private String[] headers = null;
	
    public KaggleLineProducer() {
		super();
	}

	/**
     * Create the CSVReader.
     * @return
     * @throws IOException
     */
	protected CSVReader createCSVReader() throws IOException {
		Objects.requireNonNull(properties.getProperty(CSV_READER_SPEC));
		Objects.requireNonNull(properties.getProperty(CSV_READER_GZIP));
		
    	URL url = new URL(properties.getProperty(CSV_READER_SPEC));
    	InputStream is = url.openStream();
    	InputStreamReader isr = new InputStreamReader(Boolean.valueOf(properties.getProperty(CSV_READER_GZIP)) ? new GZIPInputStream(is) : is);
		BufferedReader br = new BufferedReader(isr);
		final CSVReader csvReader = new CSVReader(br);
		// TODO Properties check for headers.
		headers = csvReader.readNext();
		return csvReader;
    }

	@Override
	protected ProducerRecord<Long, KaggleLine> start() {
		try {
			csvReader = createCSVReader();
		} catch (IOException e) {
			throw new KaggleException(e);
		}
		return new ProducerRecord<Long, KaggleLine>(TOPIC_DEFAULT, 0L, START);
	}

	@Override
	protected ProducerRecord<Long, KaggleLine> end() {
		if (csvReader != null) {
			try {
				csvReader.close();
			} catch (IOException e) {
				System.err.println(e);
			}
		}
		return new ProducerRecord<Long, KaggleLine>(TOPIC_DEFAULT, -1L, END);
	}

	@Override
	protected Iterator<ProducerRecord<Long, KaggleLine>> iterator() {
		return new Iterator<ProducerRecord<Long,KaggleLine>>() {

			protected long id = 0;
			
			protected String[] line = null;
			
			@Override
			public boolean hasNext() {
				try {
					if (line == null) {
						line = csvReader.readNext();
					}
					return line != null;
				} catch (IOException e) {
					throw new KaggleException(e);
				}
			}

			@Override
			public ProducerRecord<Long, KaggleLine> next() {
				id++;
				final ProducerRecord<Long, KaggleLine> producerRecord = new ProducerRecord<Long, KaggleLine>(TOPIC_DEFAULT, id, new KaggleLine(id, line));
				line = null;
				return producerRecord;
			}
		};
	}
}
