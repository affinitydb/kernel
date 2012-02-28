/**************************************************************************************

Copyright © 2004-2012 VMware, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

Written by Mark Venguerov 2004-2012

**************************************************************************************/

#include "ftindex.h"

using namespace AfyKernel;

const char *AfyKernel::defaultEnglishStopWords[] =
{
    "about", "ain't", "all", "also", "am", "an", "and", "any", "anybody", "anyhow", "anyone",
	"anything", "anyway", "anyways", "anywhere", "are", "aren't", "as", "at", "be", "because",
	"been", "being", "both", "but", "by", "can", "can't", "cannot", "cant", "could", "couldn't", 
	"did", "didn't", "do", "does", "doesn't", "doing", "don't", "done", "each",
    "either", "else", "etc", "ever", "every", "for", "from", "get", "gets", "getting",
    "got", "gotten", "had","hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he's", 
	"her", "here", "here's", "hers", "herself", "hi", "him", "himself", "his", "how", "i'd", "i'll", 
	"i'm", "i've", "ie", "if", "in", "into", "is", "isn't", "it", "it'd", "it'll", "it's", "its", 
	"itself", "just", "let's", "like", "mainly", "many", "may", "maybe", "me", "might", "more",
	"much", "my", "myself", "new", "no", "non", "none", "not", "nothing", "now", "of", "off", "ok",
	"okay", "on", "one", "only", "or", "other", "others", "ought", "our", "ours", "ourselves", "out",
	"per", "same", "shall", "she", "she's", "she'll", "should", "shouldn't", "so", "some", "somebody",
	"somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "such",
	"th", "than", "that", "that's", "thats", "the", "their", "theirs", "them", "themselves", "then",
    "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
	"this", "those", "though", "through", "thru", "to", "too", "until", "unto", "up",
    "upon", "us", "very", "was", "wasn't", "we", "we'd", "we'll","we're", "we've", "were",
    "weren't", "what", "what's", "when", "where", "where's", "whether", "which", "while", 
	"who", "who's", "whoever", "whom", "whose", "why", "will", "with", "within", "without", "won't", 
	"would", "wouldn't", "yes", "yet", "you", "you'd", "you'll", "you're","you've", "your", "yours",
	"yourself", "yourselves",
};

ulong AfyKernel::defaultEnglishStopWordsLen()
{
	return sizeof(defaultEnglishStopWords)/sizeof(defaultEnglishStopWords[0]);
}
/* This is the Porter stemming algorithm, it follows the
   algorithm presented in

   Porter, 1980, An algorithm for suffix stripping, Program, Vol. 14, no. 3, pp 130-137,
*/

#define	ends2(a,b)				(const char*)end-2>=beg && end[0]==b && end[-1]==a
#define	ends3(a,b,c)			(const char*)end-3>=beg && end[0]==c && end[-1]==b && end[-2]==a
#define	ends4(a,b,c,d)			(const char*)end-4>=beg && end[0]==d && end[-1]==c && end[-2]==b && end[-3]==a
#define	ends5(a,b,c,d,e)		(const char*)end-5>=beg && end[0]==e && end[-1]==d && end[-2]==c && end[-3]==b && end[-4]==a

#define	ends3m(a,b,c)			(const char*)end-3>=beg && end[0]==c && end[-1]==b && end[-2]==a && m(3)>0
#define	ends4m(a,b,c,d)			(const char*)end-4>=beg && end[0]==d && end[-1]==c && end[-2]==b && end[-3]==a && m(4)>0
#define	ends5m(a,b,c,d,e)		(const char*)end-5>=beg && end[0]==e && end[-1]==d && end[-2]==c && end[-3]==b && end[-4]==a && m(5)>0
#define	ends6m(a,b,c,d,e,f)		(const char*)end-6>=beg && end[0]==f && end[-1]==e && end[-2]==d && end[-3]==c && end[-4]==b && end[-5]==a && m(6)>0
#define	ends7m(a,b,c,d,e,f,g)	(const char*)end-7>=beg && end[0]==g && end[-1]==f && end[-2]==e && end[-3]==d && end[-4]==c && end[-5]==b && end[-6]==a && m(7)>0

#define	ends3m1(a,b,c)			(const char*)end-3>=beg && end[0]==c && end[-2]==a && m(3)>0
#define	ends4m1(a,b,c,d)		(const char*)end-4>=beg && end[0]==d && end[-2]==b && end[-3]==a && m(4)>0
#define	ends5m1(a,b,c,d,e)		(const char*)end-5>=beg && end[0]==e && end[-2]==c && end[-3]==b && end[-4]==a && m(5)>0
#define	ends6m1(a,b,c,d,e,f)	(const char*)end-6>=beg && end[0]==f && end[-2]==d && end[-3]==c && end[-4]==b && end[-5]==a && m(6)>0
#define	ends7m1(a,b,c,d,e,f,g)	(const char*)end-7>=beg && end[0]==g && end[-2]==e && end[-3]==d && end[-4]==c && end[-5]==b && end[-6]==a && m(7)>0

#define	trunc2(a,b)				if ((const char*)end-2>=beg && end[0]==b) {if (m(2)>1) end-=2; fStep5=true; break;}
#define	trunc3(a,b,c)			if ((const char*)end-3>=beg && end[0]==c && end[-2]==a) {if (m(3)>1) end-=3; fStep5=true; break;}
#define	trunc4(a,b,c,d)			if ((const char*)end-4>=beg && end[0]==d && end[-2]==b && end[-3]==a) {if (m(4)>1) end-=4; fStep5=true; break;}
#define	trunc5(a,b,c,d,e)		if ((const char*)end-5>=beg && end[0]==e && end[-2]==c && end[-3]==b && end[-4]==a) {if (m(5)>1) end-=5; fStep5=true; break;}

#define	setE(a)					(end-=a,move(),end[0]='e')

const char *AfyKernel::PorterStemmer::process(const char *word,size_t& len,char *buf)
{
	struct StemInfo {
		const char	*beg;
		char		*end;
		char		*buf;
		bool		fMove;
		StemInfo(const char *word,size_t l,char *bf) : beg(word),end((char*)word+l-1),buf(bf),fMove(word!=(const char*)buf) {assert(l>=2);}
		const char	*stem(size_t& len) {
			// step1ab() gets rid of plurals and -ed or -ing
			if (end[0]=='s') {
				if (ends4('s','s','e','s') || ends3('i','e','s')) end-=2; 
				else if (end[-1]!='s') end--;
			}
			if (ends3('e','e','d')) {if (m(3)>0) end--;}
			else if (ends2('e','d') && hasVowel(end-2) || ends3('i','n','g') && hasVowel(end-3)) {
				end-=end[0]=='d'?2:3;
				if (ends2('a','t') || ends2('b','l') || ends2('i','z')) setE(0);
				else if (isDoubleC(end)) {if (end[0]!='l' && end[0]!='s' && end[0]!='z') end--;}
				else if (m(0)==1 && isCVC(end)) setE(0);
			}
			// step1c() turns terminal y to i when there is another vowel in the stem
			if (end[0]=='y' && hasVowel(end)) {move(); end[0]='i';}
			// step2() maps double suffices to single ones
			switch (end[-1]) {
			case 'a': 
				if (ends7m1('a','t','i','o','n','a','l')) setE(4);
				else if (ends6m1('t','i','o','n','a','l')) end-=2;
				break;
			case 'c':
				if (ends4m1('e','n','c','i') || ends4m1('a','n','c','i')) setE(0);
				break;
			case 'e':
				if (ends4m1('i','z','e','r')) end--;
				break;
			case 'l':
				if (ends3m1('b','l','i')) setE(0);
				else if (ends4m1('a','l','l','i') || ends5m1('e','n','t','l','i') ||
					ends3m1('e','l','i') || ends5m1('o','u','s','l','i')) end-=2;
				break;
			case 'o':
				if (ends7m1('i','z','a','t','i','o','n')) setE(4);
				else if (ends5m1('a','t','i','o','n')) setE(2);
				else if (ends4m1('a','t','o','r')) setE(1);
				break;
			case 's':
				if (ends5m1('a','l','i','s','m')) end-=3;
				else if (ends7m1('i','v','e','n','e','s','s') || ends7m1('f','u','l','n','e','s','s')
					|| ends7m1('o','u','s','n','e','s','s')) end-=4;
				break;
			case 't':
				if (ends5m1('a','l','i','t','i')) end-=3;
				else if (ends5m1('i','v','i','t','i')) setE(2);
				else if (ends6m1('b','i','l','i','t','i')) {setE(3); end[-1]='l';}
				break;
			case 'g':
				if (ends4m1('l','o','g','i')) end--;
				break;
			}
			// step3() deals with -ic-, -full, -ness etc. similar strategy to step2
			switch (end[0]) {
			case 'e': 
				if (ends5m('i','c','a','t','e')) end-=3;
				else if (ends5m('a','t','i','v','e')) end-=5;
				else if (ends5m('a','l','i','z','e')) end-=3;
				break;
			case 'i': 
				if (ends5m('i','c','i','t','i')) end-=3;
				break;
			case 'l':
				if (ends4m('i','c','a','l')) end-=2;
				else if (ends3m('f','u','l')) end-=3;
				break;
			case 's': 
				if (ends4m('n','e','s','s')) end-=4;
				break;
			}
			// step4() takes off -ant, -ence etc., in context <c>vcvc<v>

			bool fStep5 = false;
			switch (end[-1]) {
			case 'a': trunc2('a','l'); break;
			case 'c': trunc4('a','n','c','e') else trunc4('e','n','c','e'); break;
			case 'e': trunc2('e','r'); break;
			case 'i': trunc2('i','c'); break;
			case 'l': trunc4('a','b','l','e') else trunc4('i','b','l','e'); break;
			case 'n': trunc3('a','n','t') else trunc5('e','m','e','n','t')
				else trunc4('m','e','n','t') else trunc3('e','n','t'); break;
			case 'o': 
				trunc2('o','u') 
				else if ((const char*)end-4>=beg && (end[-3]=='s' || end[-4]=='t')) trunc3('i','o','n');
				break;
			case 's': trunc3('i','s','m'); break;
			case 't': trunc3('a','t','e') else trunc3('i','t','i'); break;
			case 'u': trunc3('o','u','s'); break;
			case 'v': trunc3('i','v','e'); break;
			case 'z': trunc3('i','z','e'); break;
			}
			// step5() removes a final -e if m() > 1, and changes -ll to -l if m() > 1
			if (fStep5) {
				if (end[0]=='e') {int a=m(0); if (a>1 || a==1 && !isCVC(end-1)) end--;}
				if (end[0]=='l' && isDoubleC(end) && m(0)>1) end--;
			}
			len=end-beg+1; return beg;
		}
		void move() {if (fMove) {size_t l=end-beg; memcpy(buf,beg,l+1); beg=buf; end=buf+l; fMove=false;}}
		bool cons(const char *p) const {
			switch (*p) {
			case 'a': case 'e': case 'i': case 'o': case 'u': return false;
			case 'y': if (p>beg) return !cons(p-1); break;
			}
			return true;
		}
		int m(int k) const {
            int n=0; const char *p=beg,*pe=end-k;
			for (;;p++) {
				if (p>pe) return n;
				if (!cons(p)) break;
			}
			for (p++;;p++) {
				for (;;p++) {
					if (p>pe) return n;
					if (cons(p)) break;
				}
				n++;
				for (p++;;p++) {
					if (p>pe) return n;
					if (!cons(p)) break;
				}
			}
		}
		bool hasVowel(const char *pe) const {for (const char *p=beg; p<=pe; p++) if (!cons(p)) return true; return false;}
		bool isDoubleC(const char *p) const {return p>beg && p[-1]==p[0] && cons(p);}
		bool isCVC(const char *p) const {return p>=beg+2 && cons(p) && !cons(p-1) && cons(p-2) && p[0]!='w' && p[0]!='x' && p[0]!='y';}
	};
//	if (len<2) 
		return word;
	StemInfo si(word,len,buf);
	return si.stem(len);
}
