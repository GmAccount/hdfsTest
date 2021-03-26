package hdfs;

import java.io.IOException;
import java.util.Arrays;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class hdfsbl {

	public static void main(String[] args) throws Exception{

		hdfs_operations hdfs_obj = new hdfs_operations();		

		try{
			
		
		FileSystem fs = hdfs_obj.openCnx();
		
		String[] resList= {} ;
		System.out.println("==================");
		resList = hdfs_obj.lister(fs,"/training/exercises/filesystem/");
		System.out.println(resList.length);
		// question (1-a) 
		for (int i = 0; i < resList.length; i++) {
			  System.out.println(resList[i]);
			  hdfs_obj.getFile(fs,"/training/exercises/filesystem/"+resList[i],"/tmp/");
			}
		// question (1-b) 
		for (int i = 0; i < resList.length; i++) {
			  System.out.println(resList[i]);
			  hdfs_obj.getFile(fs,"/training/exercises/filesystem/"+resList[i],"/home/hadoop/Training/play_area/exercises/filesystem/e1/");
			}	
		
		// question (1-c)
		String[] resList_txt= {} ;
		resList_txt = hdfs_obj.listerRegex(fs,"/training/exercises/filesystem/*.txt");
		System.out.println(resList_txt.length);
		try{
		File theDir = new File("/tmp/txt_files/");
			if (!theDir.exists()){
			    theDir.mkdirs();
			}
		}catch (Exception e) {
			System.out.println("Cannot make local FS /tmp/txt_files/");
		}
		
		for (int i = 0; i < resList_txt.length; i++) {
			  System.out.println(resList_txt[i]);
			  hdfs_obj.getFile(fs,"/training/exercises/filesystem/"+resList_txt[i],"/tmp/txt_files/");
			}
		
		System.out.println("==================");
	}catch (Exception e) {

	    System.out.println("Something went wrong when copying to local file system...");
	    System.out.println(e.toString());

        }
		
		/*
		 * 
		 * 
		 * Partie 2/
		 * 
		 */
		
		// question 2-a / 2-b
		try{
			
			String[] resList_txt= {} ;

			FileSystem fs = hdfs_obj.openCnx();
			resList_txt = hdfs_obj.listFilesForFolder("/home/hadoop/Training/play_area/exercises/filesystem/e2/");
			
			for (int i = 0; i < resList_txt.length; i++) {
				
				  if (!resList_txt[i].endsWith(".txt"))
					  continue;	
				  
				  System.out.println(resList_txt[i]);
				  
				  
				  
				  
				  
				  
				  try{
				  hdfs_obj.putFile(fs,"/home/hadoop/Training/play_area/exercises/filesystem/e2/"+resList_txt[i],"/training/playArea/filesystem/e2/");
				  } catch (Exception e) {

					    System.out.println("Something went wrong when moving file !!"+resList_txt[i]+" 2 HDFS ");
					    System.out.println(e.toString());

				  }finally{
					    System.out.println("File "+resList_txt[i]+" moved succefully !! 2 HDFS ");
				  }
				  
			}		
			
		

		}catch (Exception e) {

		    System.out.println("Something went wrong when moving files !!");
		    System.out.println(e.toString());

	    }			

	}
}

class hdfs_operations{
    String param0_test;
    int param1_test;

    FileSystem openCnx() throws Exception{
    Configuration conf = new Configuration();

	conf.addResource(new Path("/home/hadoop/Training/CDH4/hadoop-2.0.0-cdh4.0.0/conf/core-site.xml"));
	FileSystem fs = FileSystem.get(conf);
	return fs;

    }
    
    
    public String[]  listFilesForFolder(String path_param) throws Exception{
    	String[] retFiles={};

    		final File folder = new File(path_param);

    		int i = 0;
    		for (final File file : folder.listFiles()) {
    			
    			retFiles = Arrays.copyOf(retFiles, retFiles.length + 1);
    			retFiles[retFiles.length - 1] = file.getName(); 
    		    System.out.println(Arrays.toString(retFiles));
    			i++;

    		}
    		return retFiles;
            
        
    }    
    
    public  String[] lister(FileSystem fs, String path_param)throws Exception{

		Path path = new Path(path_param);

		String[] retFiles={};
		FileStatus [] files = fs.listStatus(path);

		int i = 0;
		for (FileStatus file : files ){
			
			// System.out.println(file.getPath().getName());
			retFiles = Arrays.copyOf(retFiles, retFiles.length + 1);
			retFiles[retFiles.length - 1] = file.getPath().getName(); 
		    // System.out.println(Arrays.toString(retFiles));
			i++;

		}
		return retFiles;
    }

    public  String[] listerRegex(FileSystem fs, String path_param)throws Exception{

		Path path = new Path(path_param);

		String[] retFiles={};
		FileStatus [] files = fs.globStatus(path);

		int i = 0;
		for (FileStatus file : files ){
			
			// System.out.println(file.getPath().getName());
			retFiles = Arrays.copyOf(retFiles, retFiles.length + 1);
			retFiles[retFiles.length - 1] = file.getPath().getName(); // Assign 40 to the last element
		    // System.out.println(Arrays.toString(retFiles));
			i++;

		}
		return retFiles;
    }
    void putFile(FileSystem fs, String srcFile, String destFile)throws Exception{
		Path fromLocal = new Path(srcFile);
		Path toHdfs = new Path(destFile);
		fs.copyFromLocalFile(fromLocal, toHdfs);

    }
    
    void getFile(FileSystem fs, String srcFile, String destFile)throws Exception{
		Path fromLocal = new Path(srcFile);
		Path toHdfs = new Path(destFile);
		//fs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
		fs.copyToLocalFile(false,fromLocal, toHdfs,true);

    }
    
}

    


