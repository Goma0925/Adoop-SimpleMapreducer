package ao.adoop.mapreduce;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MultipleInputs {
	public static void addInputPath(Job job, Path inputPath, Class<? extends Mapper> mapperClass) {		
		if (!Files.exists(inputPath)) {
			throw new InvalidPathException(inputPath.toAbsolutePath().toString(), "Input directory or file does not exists.");
		}
		if (Files.isDirectory(inputPath)) {
			for (File fileOrDir: inputPath.toFile().listFiles()) {
				if (fileOrDir.isFile()) {
					job.addInputAndMapperPair(Paths.get((fileOrDir).getAbsolutePath()), mapperClass);
				}
			}
		}else {
			job.addInputAndMapperPair(inputPath, mapperClass);
		}
	}
}
