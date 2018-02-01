package org.twocows.yellowdog.kaggle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.twocows.yellowdog.kafka.SimpleKafkaProducer;

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
	
	public static final String CSV_READER_MAX_COUNT = "kaggle.csvreader.max.count";
	
	public static Properties append(final Properties properties, final String spec, final Boolean gzip) {
		properties.setProperty(CSV_READER_SPEC, spec);
		properties.setProperty(CSV_READER_GZIP, gzip.toString());
		return properties;
	}

	private ZipFile zipFile;
	
	private CSVReader csvReader = null;

	private String[] headers = null;
	
	private String group = UUID.randomUUID().toString();
	
    public KaggleLineProducer() {
		super();
	}

	/**
     * Create the CSVReader.
     * @return
     * @throws IOException
     */
	protected CSVReader createCSVReader() throws IOException {
		final String spec = Objects.requireNonNull(properties.getProperty(CSV_READER_SPEC));
		final boolean zip = Boolean.valueOf(properties.getProperty(CSV_READER_GZIP, Boolean.FALSE.toString()));

		InputStream is;
		if (zip) {
			zipFile = new ZipFile(spec);
			final ZipEntry zipEntry = zipFile.getEntry(spec.substring(spec.lastIndexOf("/") + 1, spec.lastIndexOf(".")));
			is = zipFile.getInputStream(zipEntry);
		} else {
			URL url = new URL(properties.getProperty(CSV_READER_SPEC));
			is = url.openStream();
		}
    	InputStreamReader isr = new InputStreamReader(is);
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
		return new ProducerRecord<Long, KaggleLine>(TOPIC_DEFAULT, 0L, new KaggleLine(group, 0, new String[0]));
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
		if (zipFile != null) {
			try {
				zipFile.close();
			} catch (IOException e) {
				System.err.println(e);
			}
		}
		return new ProducerRecord<Long, KaggleLine>(TOPIC_DEFAULT, -1L, new KaggleLine(group, -1, new String[0]));
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
				final ProducerRecord<Long, KaggleLine> producerRecord = new ProducerRecord<Long, KaggleLine>(TOPIC_DEFAULT, id, new KaggleLine(group, id, line));
				line = null;
				return producerRecord;
			}
		};
	}

	@Override
	public String toString() {
		return group + " " + count;
	}
	
	
}
