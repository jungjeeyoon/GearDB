#include <stdio.h>

int main(void){
	FILE* fin, *fout;
	fin = fopen("out-static/out","r");
	fout = fopen("parser.txt","w");
	char buf[1000];
	while(fgets(buf,1000,fin)){
		if(buf[8] == '0' || buf[11] == '0' || buf[12] == '0'){
			fputs(buf,fout);
		}
	}
	fclose(fin);
	fclose(fout);

}
