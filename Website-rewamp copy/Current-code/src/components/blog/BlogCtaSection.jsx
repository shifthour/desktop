import React from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';
import { Button } from "@/components/ui/button";
import { ArrowRight } from 'lucide-react';
import EditableBackgroundImage from '@/components/EditableBackgroundImage';
import EditableText from '@/components/EditableText';

export default function BlogCtaSection({
  textKeyPrefix,
  defaultTitle,
  defaultDescription,
  defaultButtonText,
  defaultButtonLink,
  defaultImage,
}) {
  return (
    <div className="my-8 md:my-12">
      <div className="relative overflow-hidden rounded-2xl min-h-[250px] md:min-h-[300px] flex items-center justify-center p-6 md:p-10">
        <EditableBackgroundImage
          imageKey={`${textKeyPrefix}-bg`}
          src={defaultImage}
          className="absolute inset-0 bg-cover bg-center"
        >
          <div className="absolute inset-0 bg-black/60" />
        </EditableBackgroundImage>

        <div className="relative z-10 text-center max-w-2xl">
          <EditableText
            textKey={`${textKeyPrefix}-title`}
            defaultContent={defaultTitle}
            as="h3"
            className="text-xl md:text-3xl font-bold text-white mb-3 md:mb-4 leading-tight"
          />
          <EditableText
            textKey={`${textKeyPrefix}-description`}
            defaultContent={defaultDescription}
            as="p"
            multiline
            className="text-white/90 text-sm md:text-lg mb-4 md:mb-6"
          />
          <Link to={createPageUrl(defaultButtonLink)}>
            <Button size="lg" className="bg-orange-500 text-white hover:bg-orange-600 group text-sm md:text-base">
              <EditableText
                textKey={`${textKeyPrefix}-button`}
                defaultContent={defaultButtonText}
                as="span"
              />
              <ArrowRight className="w-4 h-4 md:w-5 md:h-5 ml-2 group-hover:translate-x-1 transition-transform" />
            </Button>
          </Link>
        </div>
      </div>
    </div>
  );
}