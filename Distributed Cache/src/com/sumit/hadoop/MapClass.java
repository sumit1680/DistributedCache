package com.sumit.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends
  	Mapper<LongWritable, Text, Text, Text> {

	private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
	private BufferedReader brReader;
	private String strDeptName = "";
	private Text txtMapOutputKey = new Text("");
	private Text txtMapOutputValue = new Text("");

	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim().equals("departments_txt")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDepartmentsHashMap(eachPath, context);
			}
		}

	}

	private void loadDepartmentsHashMap(Path filePath, Context context)
			throws IOException {

		String strLineRead = "";

		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
				String deptFieldArray[] = strLineRead.split("\\t");
				DepartmentMap.put(deptFieldArray[0].trim(),
						deptFieldArray[1].trim());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		}finally {
			if (brReader != null) {
				brReader.close();

			}

		}
}
}
