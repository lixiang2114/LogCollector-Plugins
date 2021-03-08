import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestMain {

	public static void main(String[] args) {
		String[] array=new String[]{"zhang","san","hua"};
		List<String> list=Arrays.stream(array).collect(Collectors.toList());
		System.out.println(list.size());
		System.out.println(list.remove(1));
		System.out.println(list.size());
	}
}
