import React, { useState, useCallback } from 'react';
import { Upload, Download, AlertCircle, CheckCircle, X, FileSpreadsheet } from 'lucide-react';
import * as XLSX from 'xlsx';

const GarlicParserApp = () => {
  const [files, setFiles] = useState([]);
  const [processing, setProcessing] = useState(false);
  const [results, setResults] = useState(null);
  const [errors, setErrors] = useState([]);

  // 파싱 규칙 정의
  const parseGarlicOption = (optionText, productName, platform) => {
    if (!optionText && !productName) return null;
    
    const text = optionText || productName || '';
    const result = {
      category: '',
      variety: '',
      size: '',
      processing: '',
      weight: '',
      unit: '',
      parsedText: '',
      original: text
    };

    // 무게 추출 및 계산
    const extractWeight = (str) => {
      // 수식 패턴 (1kg + 1kg, 1kg x 3개 등)
      const formulaMatch = str.match(/(\d+)kg\s*[+x×]\s*(\d+)kg/i) || 
                          str.match(/(\d+)kg\s*[x×]\s*(\d+)개/i);
      if (formulaMatch) {
        const num1 = parseInt(formulaMatch[1]);
        const num2 = parseInt(formulaMatch[2]);
        return `${num1 * num2}kg`;
      }
      
      // 박스/팩 단위 계산
      const boxMatch = str.match(/(\d+)박스.*?(\d+)개/i);
      if (boxMatch) {
        return `${parseInt(boxMatch[1])}박스`;
      }
      
      const packMatch = str.match(/(\d+)g\s*x\s*(\d+)팩/i);
      if (packMatch) {
        return `${parseInt(packMatch[2])}팩`;
      }
      
      // 일반 무게 패턴
      const weightMatch = str.match(/(\d+(?:\.\d+)?)(kg|g|개|박스|팩)/i);
      if (weightMatch) {
        return weightMatch[1] + weightMatch[2].toLowerCase();
      }
      
      return '';
    };

    // 품종 추출
    const extractVariety = (str) => {
      if (str.includes('육쪽') || str.includes('명품형')) return '육쪽';
      if (str.includes('대서') || str.includes('실속형')) return '대서';
      return '';
    };

    // 크기 추출
    const extractSize = (str) => {
      const sizes = ['특대', '대', '중', '소'];
      for (const size of sizes) {
        if (str.includes(size)) return size;
      }
      return '';
    };

    // 손질유무 추출
    const extractProcessing = (str) => {
      if (str.includes('꼭지제거') || str.includes('꼭지 제거')) return '꼭지제거';
      if (str.includes('통째로') || str.includes('꼭지포함')) return '통째로';
      return '';
    };

    // 카테고리 분류
    if (text.includes('빠삭이')) {
      result.category = '마늘빠삭이';
      result.weight = extractWeight(text);
      result.parsedText = `${result.category} ${result.weight}`;
    } else if (text.includes('닭발') || text.includes('무뼈닭발')) {
      result.category = '마늘닭발';
      result.weight = extractWeight(text);
      result.parsedText = `${result.category} ${result.weight}`;
    } else if (text.includes('가루')) {
      result.category = '마늘가루';
      result.weight = extractWeight(text);
      result.parsedText = `${result.category} ${result.weight}`;
    } else if (text.includes('통마늘')) {
      result.category = '통마늘';
      result.variety = extractVariety(text);
      result.weight = extractWeight(text);
      result.parsedText = `${result.variety} ${result.category} ${result.weight}`.trim();
    } else if (text.includes('다진마늘')) {
      result.category = '다진마늘';
      result.variety = extractVariety(text);
      result.processing = extractProcessing(text);
      result.weight = extractWeight(text);
      
      // 다진마늘 특수 규칙 적용
      if (result.processing === '통째로' || text.includes('꼭지포함')) {
        result.parsedText = `${result.variety} 통째로 다진마늘 ${result.weight}`.trim();
      } else if (result.processing === '꼭지제거') {
        result.parsedText = `${result.variety} 꼭지제거 다진마늘 ${result.weight}`.trim();
      } else {
        result.parsedText = `${result.variety} ${result.category} ${result.weight}`.trim();
      }
    } else if (text.includes('깐마늘')) {
      result.category = '깐마늘';
      result.variety = extractVariety(text);
      result.size = extractSize(text);
      result.processing = extractProcessing(text);
      result.weight = extractWeight(text);
      
      // 10kg 상품의 경우 업소용 태그 추가 (제일 앞에)
      const businessTag = result.weight.includes('10kg') ? '**업소용** ' : '';
      result.parsedText = `${businessTag}${result.variety} ${result.size} ${result.processing} ${result.weight}`.trim();
    }

    return result.parsedText ? result : null;
  };

  // 컬럼 매핑 함수
  const getOptionColumn = (headers) => {
    const optionColumns = ['옵션정보', '등록옵션명', '옵션', '옵션1'];
    for (const col of optionColumns) {
      const index = headers.findIndex(h => h && h.toString().trim() === col);
      if (index !== -1) return { name: col, index };
    }
    return null;
  };

  const getProductNameColumn = (headers) => {
    const nameColumns = ['상품명', '등록상품명', '상품이름'];
    for (const col of nameColumns) {
      const index = headers.findIndex(h => h && h.toString().trim() === col);
      if (index !== -1) return { name: col, index };
    }
    return null;
  };

  // 합배송 판단 함수
  const isSameShipping = (row1, row2, nameCol, addressCol, phoneCol) => {
    if (!nameCol || !addressCol || !phoneCol) return false;
    
    const name1 = row1[nameCol.index]?.toString().trim();
    const address1 = row1[addressCol.index]?.toString().trim();
    const phone1 = row1[phoneCol.index]?.toString().trim();
    
    const name2 = row2[nameCol.index]?.toString().trim();
    const address2 = row2[addressCol.index]?.toString().trim();
    const phone2 = row2[phoneCol.index]?.toString().trim();
    
    return name1 === name2 && address1 === address2 && phone1 === phone2;
  };

  // 파일 처리 함수
  const processFiles = async () => {
    if (files.length === 0) {
      setErrors(['파일을 선택해주세요.']);
      return;
    }

    setProcessing(true);
    setErrors([]);
    const allData = [];
    const processingErrors = [];

    try {
      for (const file of files) {
        try {
          const fileData = await readExcelFile(file);
          const headers = fileData[0] || [];
          
          // 옵션 컬럼 찾기
          const optionCol = getOptionColumn(headers);
          const productNameCol = getProductNameColumn(headers);
          
          if (!optionCol && !productNameCol) {
            processingErrors.push(`${file.name}: 옵션 또는 상품명 컬럼을 찾을 수 없습니다.`);
            continue;
          }

          // 배송 정보 컬럼 찾기 (합배송 판단용)
          const nameCol = headers.findIndex(h => h && (h.includes('이름') || h.includes('성명')));
          const addressCol = headers.findIndex(h => h && h.includes('주소'));
          const phoneCol = headers.findIndex(h => h && (h.includes('전화') || h.includes('휴대폰')));

          // 데이터 처리
          for (let i = 1; i < fileData.length; i++) {
            const row = fileData[i];
            if (!row || row.length === 0) continue;

            const optionText = optionCol ? row[optionCol.index] : '';
            const productName = productNameCol ? row[productNameCol.index] : '';
            
            // 파싱 실행
            const parsed = parseGarlicOption(optionText, productName, file.name);
            
            if (parsed) {
              const processedRow = [...row];
              processedRow.push(parsed.parsedText); // 파싱된 옵션
              processedRow.push(file.name); // 파일명
              processedRow.push(i); // 원본 행 번호
              
              allData.push({
                row: processedRow,
                original: row,
                fileName: file.name,
                rowIndex: i,
                parsedOption: parsed.parsedText,
                nameIndex: nameCol,
                addressIndex: addressCol,
                phoneIndex: phoneCol
              });
            }
          }
        } catch (fileError) {
          processingErrors.push(`${file.name}: ${fileError.message}`);
        }
      }

      // 합배송 그룹핑
      const shippingGroups = [];
      const processed = new Set();

      for (let i = 0; i < allData.length; i++) {
        if (processed.has(i)) continue;
        
        const group = [allData[i]];
        processed.add(i);
        
        for (let j = i + 1; j < allData.length; j++) {
          if (processed.has(j)) continue;
          
          if (isSameShipping(
            allData[i].row, 
            allData[j].row,
            { index: allData[i].nameIndex },
            { index: allData[i].addressIndex },
            { index: allData[i].phoneIndex }
          )) {
            group.push(allData[j]);
            processed.add(j);
          }
        }
        
        shippingGroups.push(group);
      }

      // 합배송 그룹을 상단으로, 단일 배송을 하단으로 정렬
      const combinedShipping = shippingGroups.filter(group => group.length > 1);
      const singleShipping = shippingGroups.filter(group => group.length === 1);
      
      const finalData = [...combinedShipping.flat(), ...singleShipping.flat()];
      
      setResults({
        data: finalData,
        totalRows: finalData.length,
        combinedShippingCount: combinedShipping.length,
        singleShippingCount: singleShipping.length,
        headers: files[0] ? await getHeaders(files[0]) : []
      });

      if (processingErrors.length > 0) {
        setErrors(processingErrors);
      }

    } catch (error) {
      setErrors([`처리 중 오류가 발생했습니다: ${error.message}`]);
    } finally {
      setProcessing(false);
    }
  };

  // Excel 파일 읽기
  const readExcelFile = (file) => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const data = new Uint8Array(e.target.result);
          const workbook = XLSX.read(data, { type: 'array' });
          const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
          const jsonData = XLSX.utils.sheet_to_json(firstSheet, { header: 1 });
          resolve(jsonData);
        } catch (error) {
          reject(new Error(`Excel 파일 읽기 실패: ${error.message}`));
        }
      };
      reader.onerror = () => reject(new Error('파일 읽기 실패'));
      reader.readAsArrayBuffer(file);
    });
  };

  // 헤더 가져오기
  const getHeaders = async (file) => {
    const data = await readExcelFile(file);
    return data[0] || [];
  };

  // 파일 업로드 핸들러
  const handleFileUpload = useCallback((e) => {
    const uploadedFiles = Array.from(e.target.files);
    const excelFiles = uploadedFiles.filter(file => 
      file.name.endsWith('.xlsx') || file.name.endsWith('.xls')
    );
    
    if (excelFiles.length !== uploadedFiles.length) {
      setErrors(['Excel 파일(.xlsx, .xls)만 업로드 가능합니다.']);
    }
    
    setFiles(excelFiles);
    setResults(null);
    setErrors([]);
  }, []);

  // 파일 제거
  const removeFile = (index) => {
    const newFiles = files.filter((_, i) => i !== index);
    setFiles(newFiles);
    if (newFiles.length === 0) {
      setResults(null);
    }
  };

  // 결과 다운로드
  const downloadResults = () => {
    if (!results) return;

    const headers = [...results.headers, '파싱된 옵션', '파일명', '원본 행번호'];
    const data = [headers, ...results.data.map(item => item.row)];
    
    const ws = XLSX.utils.aoa_to_sheet(data);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, '통합 결과');
    
    const fileName = `마늘주문_통합결과_${new Date().toISOString().slice(0, 10)}.xlsx`;
    XLSX.writeFile(wb, fileName);
  };

  return (
    <div className="max-w-6xl mx-auto p-6 bg-white">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          마늘 주문 파일 파싱 통합 시스템
        </h1>
        <p className="text-gray-600">
          여러 쇼핑몰의 마늘 주문 Excel 파일을 업로드하여 옵션 정보를 파싱하고 통합된 파일로 다운로드하세요.
        </p>
      </div>

      {/* 파일 업로드 */}
      <div className="mb-6">
        <div className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center hover:border-gray-400 transition-colors">
          <Upload className="mx-auto h-12 w-12 text-gray-400 mb-4" />
          <div className="mb-4">
            <label htmlFor="file-upload" className="cursor-pointer">
              <span className="text-lg font-medium text-blue-600 hover:text-blue-500">
                Excel 파일 선택
              </span>
              <input
                id="file-upload"
                name="file-upload"
                type="file"
                multiple
                accept=".xlsx,.xls"
                className="sr-only"
                onChange={handleFileUpload}
              />
            </label>
          </div>
          <p className="text-sm text-gray-500">
            여러 파일을 동시에 선택할 수 있습니다. (.xlsx, .xls 형식만 지원)
          </p>
        </div>
      </div>

      {/* 업로드된 파일 목록 */}
      {files.length > 0 && (
        <div className="mb-6">
          <h3 className="text-lg font-medium text-gray-900 mb-3">업로드된 파일</h3>
          <div className="space-y-2">
            {files.map((file, index) => (
              <div key={index} className="flex items-center justify-between bg-gray-50 p-3 rounded-lg">
                <div className="flex items-center">
                  <FileSpreadsheet className="h-5 w-5 text-green-600 mr-2" />
                  <span className="text-sm font-medium">{file.name}</span>
                  <span className="text-xs text-gray-500 ml-2">
                    ({(file.size / 1024).toFixed(1)}KB)
                  </span>
                </div>
                <button
                  onClick={() => removeFile(index)}
                  className="text-red-500 hover:text-red-700"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 오류 메시지 */}
      {errors.length > 0 && (
        <div className="mb-6">
          <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <div className="flex">
              <AlertCircle className="h-5 w-5 text-red-400 mr-2 mt-0.5" />
              <div>
                <h3 className="text-sm font-medium text-red-800 mb-1">오류 발생</h3>
                <ul className="text-sm text-red-700 space-y-1">
                  {errors.map((error, index) => (
                    <li key={index}>• {error}</li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* 처리 버튼 */}
      <div className="mb-6">
        <button
          onClick={processFiles}
          disabled={files.length === 0 || processing}
          className="w-full bg-blue-600 text-white py-3 px-4 rounded-lg font-medium hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
        >
          {processing ? '처리 중...' : '파일 처리 시작'}
        </button>
      </div>

      {/* 처리 결과 */}
      {results && (
        <div className="space-y-6">
          <div className="bg-green-50 border border-green-200 rounded-lg p-4">
            <div className="flex">
              <CheckCircle className="h-5 w-5 text-green-400 mr-2 mt-0.5" />
              <div>
                <h3 className="text-sm font-medium text-green-800 mb-1">처리 완료</h3>
                <div className="text-sm text-green-700">
                  <p>• 총 {results.totalRows}개 주문 처리</p>
                  <p>• 합배송 그룹: {results.combinedShippingCount}개</p>
                  <p>• 단일 배송: {results.singleShippingCount}개</p>
                </div>
              </div>
            </div>
          </div>

          <div className="flex justify-between items-center">
            <h3 className="text-lg font-medium text-gray-900">처리 결과 미리보기</h3>
            <button
              onClick={downloadResults}
              className="bg-green-600 text-white py-2 px-4 rounded-lg font-medium hover:bg-green-700 transition-colors flex items-center"
            >
              <Download className="h-4 w-4 mr-2" />
              Excel 다운로드
            </button>
          </div>

          <div className="border rounded-lg overflow-hidden">
            <div className="overflow-x-auto max-h-96">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">파싱된 옵션</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">파일명</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">원본 행</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">원본 데이터</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {results.data.slice(0, 10).map((item, index) => (
                    <tr key={index} className="hover:bg-gray-50">
                      <td className="px-3 py-2 text-sm font-medium text-blue-600">
                        {item.parsedOption}
                      </td>
                      <td className="px-3 py-2 text-sm text-gray-500">
                        {item.fileName}
                      </td>
                      <td className="px-3 py-2 text-sm text-gray-500">
                        {item.rowIndex}
                      </td>
                      <td className="px-3 py-2 text-sm text-gray-900 max-w-xs truncate">
                        {item.original.slice(0, 3).join(' | ')}...
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {results.data.length > 10 && (
              <div className="bg-gray-50 px-3 py-2 text-sm text-gray-500 text-center">
                총 {results.data.length}개 중 10개만 표시됩니다. 전체 결과는 Excel 파일을 다운로드하여 확인하세요.
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default GarlicParserApp;