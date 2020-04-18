package guru.com.Utils;

import java.io.File;

public class HelperUtilities {

	public static HelperUtilities Instance = new HelperUtilities(); 
	
	public String getResourceFilePath(String filePath)
	{
		try
		{
		File file = new File(getClass().getClassLoader().getResource(filePath).getFile());
		
		return file.getAbsolutePath();
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			return null;
		}
	}
}
