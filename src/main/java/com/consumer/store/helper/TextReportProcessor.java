package com.consumer.store.helper;

public class TextReportProcessor {


    private String text;
    private String[] headers;
    private String[][] rows;
    private int row_num;

    public TextReportProcessor(String text){
        
        this.text           = text;
        String[] text_body  = this.text.split("[\\r\\n]+");
        this.headers        = text_body[0].split("\\t");
        String[][] tmp_rows = new String[ text_body.length - 1 ][ this.headers.length ];

        for (int i = 1; i < text_body.length; i++) {
            String arr[]   = text_body[i].split("\\t");
            tmp_rows[i-1]  = arr;
        }

        this.rows = tmp_rows;
    }

    public TextReportProcessor get(int row_num){

        this.row_num = row_num;
        return this;
    }

    public String column(String col_name){

        String result = null;
        for (int i = 0; i < this.headers.length; i++) {
            String header = this.headers[i];
            if(header.equalsIgnoreCase(col_name)){
                if(i < this.rows[ this.row_num ].length){
                  result = this.rows[ this.row_num ][ i ];
                }
                break;
            }
        }
        return result;
    }
    
    public int length(){
        return this.rows.length;
    }

    
}
