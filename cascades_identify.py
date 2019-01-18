#-*-coding=utf-8-*-
import numpy as np
from datetime import datetime
import math
import pandas as pd
'''
Author: Leihan Zhang
Email: zhangleihan@gmail.com
Introduction: Identify cascades from time series with different length and value scale.
              Each cascade contains start timing, peak timing, end timing and accumulated value.
'''
class identify():
    def __init__(self, parameter):
        self.window = parameter['window']
        self.h_local = parameter['h_local']
        self.h_global = parameter['h_global']

    def getS_5(self, al, idx):
        #,idx,k,h,factor):
        al_len = len(al)
        startidx = idx-self.window if idx>self.window else 0
        endidx = idx+self.window if (idx+self.window)<al_len else al_len-1

        m = np.mean(al[startidx:endidx])
        s = np.std(al[startidx:endidx])

        return math.fabs(al[idx]-m)-self.h_local*s

    def spd_db(self, al, sidx, eidx):
        '''

        Parameters
        ----------
        al: array of time series
        h: threshold in Chebyshev inequation
        k: unit time window
        factor: factor for time window

        Returns
        -------
        result: list of peaks indexes for cascades

        '''
        oo = []
        result = []
        maxv = np.max(al[sidx:eidx])
        al_f = []
        le = len(al[sidx:eidx])
        for ai in al:
            al_f.append(1.0 * ai * le / maxv)
        #n = len(al)
        al_t = []
        for i in range(0,len(al_f)):
            al_t.append(self.getS_5(al_f,i))

        for i in range(sidx,eidx):#(0,len(al_t)): #len(al)
            if al_t[i]>0:# and (al_t[i]-mean)>h*var:
                oo.append(i)

        _ave = np.mean(al_f)#(al)
        _std = np.std(al_f)#(al)
        for ni_val in oo:
            if al_f[ni_val]>_ave+self.h_global*_std and ni_val<len(al):
                result.append(ni_val)
        return result

    def get_s_e(self, al, peak):
        '''
        Parameters
        ----------
        al: array of time series
        peak: candidate peaks for distinguishing cascades
        k: unit time window
        factor: factor for time window

        Returns
        -------
        peak: the list of indexes of peaks
        selist: the list of cascades, each element of the list is a string consisting of start idx, peak idx, end idx, accumulated value of cascade.

        '''
        maxv = np.max(al)
        #al_f = al*len(al)/maxv
        al_f = []
        le = len(al)
        for ai in al:
            al_f.append(1.0*ai*le/maxv)
        #print(al_f)
        al_len = len(al)
        result = []
        for j in range(0,len(peak)):
            pi = peak[j]
            idx1 = pi

            f1 = al_f[idx1]

            dmax1 = 0
            idx0 = idx1 - 2*self.window if idx1>2*self.window else 0
            idxe = idx1 + 2*self.window if idx1+2*self.window < al_len else al_len
            idxa1 = idx0

            for i in range(idx0,idx1):
                y = i - idx1 + f1
                dt = abs(i-al_f[i]-idx1+f1)/math.sqrt(2)
                #print(dt)
                if (dmax1 < dt and y>al_f[i]):

                    idxa1 = i
                    dmax1 = dt
            idxs1 = idxe-1
            smax1 = 0
            for i in range(idx1+1,idxe-1):
                y = -i +idx1 + f1
                dt = abs(al_f[i]+i-idx1-f1)/math.sqrt(2)
                if (smax1 < dt and y>al_f[i]):
                    idxs1 = i
                    smax1 = dt
            result.append(str(idxa1)+','+str(idx1)+','+str(idxs1))
        #start filter sbs
        seindexlist = []
        if len(result) == 1:
            seindexlist.append(0)
        else:
            i = 0
            flag = True
            while i < (len(result) - 1):
                # print(i)
                sesi = result[i].split(',')
                targetindex = i

                for j in range(i + 1, len(result)):
                    sesj = result[j].split(',')
                    if int(sesj[0]) > (int(result[targetindex].split(',')[2]) + 3):
                        seindexlist.append(targetindex)
                        i += 1
                        break
                    else:
                        #print('f:',sesj[1])

                        #print(len(al))

                        if int(sesj[1])<len(al) and al[int(sesj[1])] > al[int(result[targetindex].split(',')[1])]:
                            targetindex = j
                        i += 1

                        if j == (len(result) - 1):
                            seindexlist.append(targetindex)
                            flag = False
                            break
            if flag:
                seindexlist.append((len(result) - 1))
        peakslist = []
        selist = []
        for ti in seindexlist:
            ses = result[ti].split(',')
            sum = 0
            for i in range(int(ses[0]), int(ses[2]) + 1):
                sum += al[i]
            if sum >= 100:
                peakslist.append(int(ses[1]))
                selist.append(result[ti]+','+str(sum))
        #filtering ending
        return peakslist, selist

    def _plot(self, x, valley, ax, ind, sidx):
        """Plot results of the detect_peaks function, see its help."""
        x = np.array(x)
        #print(x)
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            sns.set()
        except ImportError:
            print('matplotlib is not available.')
        else:
            if ax is None:
                _, ax = plt.subplots(1, 1, figsize=(8, 4))

            ax.plot(x, 'b', lw=1)
            #if len(ind):
            ci = 0
            for sei in ind:
                ci +=1
                label = 'valley' if valley else 'peak'
                label = label + 's' if len(ind) > 1 else label
                ax.plot(sei, x[sei], '+', mfc=None, mec='r', mew=2, ms=8,
                        label='%d %s' % (ci, label))
                ax.axvspan(0, dif, facecolor='0.3', alpha=0.2)
                ax.legend(loc='best', framealpha=.5, numpoints=1)
            ax.set_xlim(-.02*x.size, x.size*1.02-1)
            ymin, ymax = x[np.isfinite(x)].min(), x[np.isfinite(x)].max()
            yrange = ymax - ymin if ymax > ymin else 1
            ax.set_ylim(ymin - 0.1*yrange, ymax + 0.1*yrange)
            ax.set_xlabel('Data #', fontsize=14)
            ax.set_ylabel('Amplitude', fontsize=14)
            mode = 'Valley detection' if valley else 'Peak detection'
            ax.set_title("%s (h=%f, k=%d, dif=%d )"
                         % (mode, self.h_local, self.window, sidx))
            ax.axvspan(0, sidx, facecolor='0.3', alpha=0.2)
            #plt.grid()
            plt.show()

if __name__ =="__main__":

    """
    #tuned_parameters = [{'k':[4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28], 'h_local':[1.0,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8,1.9,2.0], 'h_global':[0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]}]
    row = [504,510,650,802,1298,1149,825,769,595,438,435,1132,829,756,686,543,540,513,1220,853,652,615,600,650,705,1790,895,866,606,757,529,646,1391,919,917,838,467,542,415,886,663,722,620,528,395,383,432,243,139,149,122,154,162,93,157,267,395,319,352,266,614,567,545,444,684,425,552,808,826,1306,759,726,517,436,1748,1254,1146,981,726,537,585,1533,1044,1049,766,978,1060,1910,5600,5492,2370,940,911,427,479,1078,1309,1031,640,408,485,523,1120,786,790,592,520,404,407,834,644,409,478,422,308,342,1005,556,418,367,249,250,291,744,189,150,158,80,93,59,220,172,190,144,174,150,130,344,213,146,187,109,136,113,265,164,136,134,181,152,102,191,114,153,180,124,72,61,251,138,92,130,117,68,69,190,216,168,269,104,39,29,79,135,145,86,68,112,104,169,150,336,237,865,150,130,139,139,179,635,1031,2184,340,324,431,241,134,158,85,94,127,110,116,718,307,147,118,191,128,119,81,61,50,88,100,159,324,575,285,265,294,312,265,233,204,197,208,178,256,264,454,346,212,208,182,233,242,199,286,223,178,154,238,288,255,170,173,143,267,212,315,331,228,339,251,167,252,259,266,293,251,962,4006,6350,13887,418,226,207,138,118,553,421,233,212,156,182,152,254,224,169,1972,252,205,153,303,273,170,297,270,282,265,266,312,236,260,176,136,156,325,246,216,255,231,172,269,376,410,400,330,313,375,254,302,1012,621,344,341,199,186,273,265,183,276,279,221,208,750,256,246,211,229,153,162,220,222,201,210,229,152,135,220,237,220,311,329,219,233,800,336,197,200,224,148,129,234,216,195,277,197,101,151,146,183,258,235,78,134,131,363,283,291,222,188,67,86,177,187,145,226,223,163,179,178,175,726,344,262,160,165,142,127,129,138,277,71,94,138,102,115,83,132,96,34,34,20,45,42,36,64,80,100,133,97,142,111,81,95,95,125,239,302,196,170,154,208,226,136,223,237,180,118,178,207,167,164,19,18,11,14,31,18,26,19,19,21,89,175,161,206,201,96,102,193,133,229,212,186,78,86,79,166,282,212,266,187,144,183,127,239,341,183,136,187,425,317,200,230,217,151,108,95,205,149,173,117,88,210,78,150,197,155,176,154,92,171,136,184,230,169,152,120,187,185,291,91,283,166,120,136,123,153,38,143,75,36,41,150,51,88,181,152,83,190,174,119,92,78,109,209,143,138,133,168,170,369,285,782,2510,1148,793,613,260,225,255,204,199,232,284,176,113,207,208,176,167,133,105,156,469,356,341,253,239,127,214,193,411,297,202,256,71,82,87,97,114,91,125,105,88,122,192,171,113,91,58,48,259,421,203,158,120,105,108,196,344,145,265,142,66,82,166,159,158,141,191,101,102,104,102,187,168,122,100,396,931,221,1241,3289,193,138,126,122,111,92,67,65,75,118,80,103,112,89,104,101,79,85,81,119,121,93,63,58,44,62,99,91,79,122,523,288,178,112,125,126,93,71,120,113,79,91,96,87,77,91,231,96,116,149,96,71,126,166,108,65,106,224,99,259,148,92,340,213,65,96,536,165,198,110,118,83,82,113,138,118,118,85,75,74,112,138,132,131,123,112,75,158,283,298,229,304,325,240,281,308,155,167,74,62,85,103,51,76,135,89,69,61,102,79,75,65,124,50]
    k = 14  #time window (unit)
    """
    row = [504,510,650,802,1298,1149,825,769,595,438,435,1132,829,756,686,543,540,513,1220,853,652,615,600,650,705,1790,895,866,606,757,529,646,1391,919,917,838,467,542,415,886,663,722,620,528,395,383,432,243,139,149,122,154,162,93,157,267,395,319,352,266,614,567,545,444,684,425,552,808,826,1306,759,726,517,436,1748,1254,1146,981,726,537,585,1533,1044,1049,766,978,1060,1910,5600,5492,2370,940,911,427,479,1078,1309,1031,640,408,485,523,1120,786,790,592,520,404,407,834,644,409,478,422,308,342,1005,556,418,367,249,250,291,744,189,150,158,80,93,59,220,172,190,144,174,150,130,344,213,146,187,109,136,113,265,164,136,134,181,152,102,191,114,153,180,124,72,61,251,138,92,130,117,68,69,190,216,168,269,104,39,29,79,135,145,86,68,112,104,169,150,336,237,865,150,130,139,139,179,635,1031,2184,340,324,431,241,134,158,85,94,127,110,116,718,307,147,118,191,128,119,81,61,50,88,100,159,324,575,285,265,294,312,265,233,204,197,208,178,256,264,454,346,212,208,182,233,242,199,286,223,178,154,238,288,255,170,173,143,267,212,315,331,228,339,251,167,252,259,266,293,251,962,4006,6350,13887,418,226,207,138,118,553,421,233,212,156,182,152,254,224,169,1972,252,205,153,303,273,170,297,270,282,265,266,312,236,260,176,136,156,325,246,216,255,231,172,269,376,410,400,330,313,375,254,302,1012,621,344,341,199,186,273,265,183,276,279,221,208,750,256,246,211,229,153,162,220,222,201,210,229,152,135,220,237,220,311,329,219,233,800,336,197,200,224,148,129,234,216,195,277,197,101,151,146,183,258,235,78,134,131,363,283,291,222,188,67,86,177,187,145,226,223,163,179,178,175,726,344,262,160,165,142,127,129,138,277,71,94,138,102,115,83,132,96,34,34,20,45,42,36,64,80,100,133,97,142,111,81,95,95,125,239,302,196,170,154,208,226,136,223,237,180,118,178,207,167,164,19,18,11,14,31,18,26,19,19,21,89,175,161,206,201,96,102,193,133,229,212,186,78,86,79,166,282,212,266,187,144,183,127,239,341,183,136,187,425,317,200,230,217,151,108,95,205,149,173,117,88,210,78,150,197,155,176,154,92,171,136,184,230,169,152,120,187,185,291,91,283,166,120,136,123,153,38,143,75,36,41,150,51,88,181,152,83,190,174,119,92,78,109,209,143,138,133,168,170,369,285,782,2510,1148,793,613,260,225,255,204,199,232,284,176,113,207,208,176,167,133,105,156,469,356,341,253,239,127,214,193,411,297,202,256,71,82,87,97,114,91,125,105,88,122,192,171,113,91,58,48,259,421,203,158,120,105,108,196,344,145,265,142,66,82,166,159,158,141,191,101,102,104,102,187,168,122,100,396,931,221,1241,3289,193,138,126,122,111,92,67,65,75,118,80,103,112,89,104,101,79,85,81,119,121,93,63,58,44,62,99,91,79,122,523,288,178,112,125,126,93,71,120,113,79,91,96,87,77,91,231,96,116,149,96,71,126,166,108,65,106,224,99,259,148,92,340,213,65,96,536,165,198,110,118,83,82,113,138,118,118,85,75,74,112,138,132,131,123,112,75,158,283,298,229,304,325,240,281,308,155,167,74,62,85,103,51,76,135,89,69,61,102,79,75,65,124,50]
    #factor = 1 # factor*k represents the time window around the index
    h_local = 1  # threshold in Chebyshev for identifying peaks
    h_global = 0.5
    parameter = {'window':14,'h_local':1, 'h_global': 0.5}
    iden = identify(parameter)
    peak = iden.spd_db(row, 0, len(row))
    dif = 0  # series start time
    if len(peak)>0:
        peak, p_s_e = iden.get_s_e(row,peak)
        # peak: the list of indexes of peaks
        # selist: the list of cascades, each element of the list is a string containing:
        #         start idx, peak idx, end idx, accumulated value of cascade.
        # print(peak)
        print(p_s_e)
        iden._plot(row, False, None, peak, 0)