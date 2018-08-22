import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static List<String> splite(String csv_str) {
        ArrayList<String> ll = new ArrayList();
        if(csv_str == null || csv_str.length() == 0)
            return ll;
        try{
            int start = -1;
            int end = -1;
            int i = 0;
            int colon_num = 0;
            for(i = 0; i < csv_str.length(); i++) {
                if(csv_str.charAt(i) == '"') {
                    colon_num ++;
                    if(start == -1 && colon_num == 2) {
                        start = i;
                    }
                    if(colon_num > 0 && colon_num % 2 == 0) {
                        end = i;
                    }
                    continue;
                }
                if(start == -1)
                    start = i;
                if(csv_str.charAt(i) == ',' && colon_num % 2 == 0) {
                    if(end == -1) end = i;
                    String subStr = csv_str.substring(start, end);
                    subStr.trim();
                    ll.add(subStr);
                    while(i+1 < csv_str.length() && csv_str.charAt(i+1) == ' ')
                        i ++;
                    colon_num = 0;
                    start = -1;
                    end = -1;
                }
            }
            if(i > start) {
                if(start == -1) start = i;
                if(end == -1) end = i;
                String subStr = csv_str.substring(start, end);
                subStr.trim();
                ll.add(subStr);
            }
        }catch(Exception e){
            System.out.println(csv_str);
            e.printStackTrace();

        }
        ArrayList<String> ans = new ArrayList();
        for(int i=0;i<ll.size();i++)
            ans.add(ll.get(i).trim());

        return ans;
    }

    public static  void main(String[]args){
        String string  = "\" a\",\"32.2322322323232fsd2342432342423424242424,32232\", \"adfb,\'\"\", \",\"\",  \"中国人民kaIFA银行\",  \"2018/12/7\",12";
        List<String> result = splite(string);
        for(String s:result){
            System.out.println(s);
        }
    }

}
