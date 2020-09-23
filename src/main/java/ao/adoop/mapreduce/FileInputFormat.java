package ao.adoop.mapreduce;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class FileInputFormat {

	public static void addInputPath(Job job, Path inputPath) {
		if (!Files.exists(inputPath)) {
			throw new InvalidPathException(inputPath.toAbsolutePath().toString(), "Input directory or file does not exists.");
		}
		if (Files.isDirectory(inputPath)) {
			for (File fileOrDir: inputPath.toFile().listFiles()) {
				if (fileOrDir.isFile()) {
					job.addInputFilePath(Paths.get((fileOrDir).getAbsolutePath()));
				}
			}
		}else {
			job.addInputFilePath(inputPath);
		}
	};

}
