"""
OCR via Apple Vision framework no iPad M4.

Usa objc_util (disponível no Pyto) para acessar o framework Vision do iOS.
Para ambiente não-iPad (teste no Mac), faz fallback para PyMuPDF se disponível.
"""

import os
import logging

import config

log = logging.getLogger(__name__)


def ocr_pdf(pdf_path):
    """Extrai texto de um PDF usando Apple Vision (iPad) ou fallback."""
    if not os.path.exists(pdf_path):
        log.error("Arquivo não encontrado: %s", pdf_path)
        return ""

    # Tentar Apple Vision (disponível no Pyto/iPad)
    try:
        return _ocr_apple_vision(pdf_path)
    except ImportError:
        log.info("objc_util não disponível — tentando fallback PyMuPDF")
    except Exception as e:
        log.warning("Erro no Apple Vision OCR: %s — tentando fallback", e)

    # Fallback: PyMuPDF (funciona no Mac para testes)
    try:
        return _ocr_pymupdf(pdf_path)
    except ImportError:
        log.warning("PyMuPDF não disponível. Instale com: pip install pymupdf")
        return ""


def _ocr_apple_vision(pdf_path):
    """OCR real usando VNRecognizeTextRequest do iOS Vision framework."""
    from objc_util import ObjCClass, nsurl, ns
    import ctypes

    NSURL = ObjCClass("NSURL")
    PDFDocument = ObjCClass("PDFDocument")
    VNRecognizeTextRequest = ObjCClass("VNRecognizeTextRequest")
    VNImageRequestHandler = ObjCClass("VNImageRequestHandler")

    file_url = NSURL.fileURLWithPath_(ns(pdf_path))
    pdf_doc = PDFDocument.alloc().initWithURL_(file_url)

    if pdf_doc is None:
        log.error("Não foi possível abrir PDF: %s", pdf_path)
        return ""

    page_count = pdf_doc.pageCount()
    all_text = []

    for i in range(page_count):
        page = pdf_doc.pageAtIndex_(i)
        # Renderizar página para imagem
        page_image = page.thumbnailOfSize_forBox_(
            page.boundsForBox_(0).size,  # kPDFDisplayBoxMediaBox = 0
            0
        )

        if page_image is None:
            continue

        # Criar handler com a imagem da página
        handler = VNImageRequestHandler.alloc().initWithCGImage_options_(
            page_image.CGImage(), None
        )

        # Configurar request de OCR
        request = VNRecognizeTextRequest.alloc().init()
        request.setRecognitionLevel_(1)  # VNRequestTextRecognitionLevelAccurate = 1
        request.setUsesLanguageCorrection_(True)
        request.setRecognitionLanguages_(ns(config.OCR_LANGUAGES))

        # Executar
        error = ctypes.c_void_p()
        handler.performRequests_error_([request], ctypes.byref(error))

        results = request.results()
        if results:
            for obs in results:
                candidate = obs.topCandidates_(1).firstObject()
                if candidate:
                    all_text.append(str(candidate.string()))

    texto = "\n".join(all_text)
    log.info("OCR Vision: %s → %d chars, %d páginas", os.path.basename(pdf_path), len(texto), page_count)
    return texto


def _ocr_pymupdf(pdf_path):
    """Fallback: extração de texto com PyMuPDF (sem OCR real)."""
    import fitz  # PyMuPDF

    doc = fitz.open(pdf_path)
    all_text = []
    for page in doc:
        text = page.get_text()
        if text:
            all_text.append(text)
    doc.close()

    texto = "\n".join(all_text)
    log.info("PyMuPDF: %s → %d chars, %d páginas", os.path.basename(pdf_path), len(texto), doc.page_count)
    return texto
